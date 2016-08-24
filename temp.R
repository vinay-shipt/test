library('plyr')
library('dplyr')
library('lubridate')
library('ggplot2')
library('reshape2')
library('scales')
library('RPostgreSQL')
library('caroline')


date <- as.character(as_date(with_tz(Sys.time(), "GMT")))
print(paste0("No date parameter supplied, default to current date ", date, " in UTC."))
## VB: should replace "dt" with "date", probably never triggered since it gets run with parameter

# posixct date
p_date = as.POSIXct(date, format = '%Y-%m-%d')

  # production db credentials
  login_prod = Sys.getenv(c('PROD_RR_DB_HOST','PROD_RR_DB_SCHEMA','PROD_RR_DB_PORT','PROD_RR_DB_USER', 'PROD_RR_DB_PWD'))
  drv = dbDriver("PostgreSQL")
  # get event data
#  stripe_events = get_event_data(login_prod, drv, date)

#get_event_data = function(login, drv, date) {

    prod = dbConnect(drv, 
                   dbname=login_prod['PROD_RR_DB_SCHEMA'],host=login_prod['PROD_RR_DB_HOST'],
                   port=login_prod['PROD_RR_DB_PORT'],
                   user=login_prod['PROD_RR_DB_USER'],
                   password=login_prod['PROD_RR_DB_PWD'])
  data = dbGetQuery(prod, paste0("select billing_events.created_at time_stamp,
                                 billing_events.customer_id customer_id,
                                 customers.name as name,
                                 customers.username as email,
                                 customers.guest_account as guest,
                                 billing_events.payload->>'customer' stripe_customer_id,
                                 metros.name as metro,
                                 billing_events.payload->>'id' subscription_id,
                                 billing_events.event event_type,
                                 billing_events.payload->>'start' started_at,
                                 billing_events.payload->>'ended_at' ended_at,
                                 billing_events.payload->'plan'->>'name' membership_type,
                                 billing_events.payload->'plan'->>'interval' as interval,
                                 billing_events.payload->>'status' status,
                                 billing_events.payload->'discount'->'coupon'->>'id' coupon
                                 from billing_events
                                 left join customers on billing_events.customer_id = customers.id
                                 left join metros on customers.metro_id = metros.id
                                 where (event = 'customer.subscription.created' or event = 'customer.subscription.deleted' or event = 'customer.subscription.updated')
                                 and billing_events.created_at::date = '", date, "'
                                 order by 1"))
  dbDisconnect(prod)
stripe_events <- data


  
  
  
  
  stripe_events = stripe_events %>% mutate(started_at = as.POSIXct(as.numeric(started_at), 
                                                                   origin = '1970-01-01'), 
                                           ended_at = as.POSIXct(as.numeric(ended_at), 
                                                                 origin = '1970-01-01'))

  # data sci db credentials
  login_proc = Sys.getenv(c('DATA_DB_HOST','DATA_DB_USER','DATA_DB_PWD','DATA_DB_SCHEMA','DATA_DB_PORT'))
  # get membership data
#  membership = get_membership_data(login_proc, drv)
  
  proc = dbConnect(drv, dbname=login_proc['DATA_DB_SCHEMA'],
                   host=login_proc['DATA_DB_HOST'],
                   port=login_proc['DATA_DB_PORT'],
                   user=login_proc['DATA_DB_USER'], 
                   password=login_proc['DATA_DB_PWD'])
  data = dbReadTable(proc, 'membership_current')
  dbDisconnect(proc)
  data = data %>% select(-id)
  membership <- data

  
  # get guest data
#  guest_data = get_guest_data(login_prod, drv)
  prod = dbConnect(drv, dbname=login_prod['PROD_RR_DB_SCHEMA'],
                   host=login_prod['PROD_RR_DB_HOST'],
                   port=login_prod['PROD_RR_DB_PORT'],
                   user=login_prod['PROD_RR_DB_USER'],
                   password=login_prod['PROD_RR_DB_PWD'])
  data = dbGetQuery(prod, 'select id customer_id, updated_at from customers where guest_account is true')
  dbDisconnect(prod)
  guest_data <- data

  summarized_events = stripe_events %>%
    mutate(interval = ifelse(interval == 'month', 'monthly',
                             ifelse(interval == 'year', 'annual', interval))) 
  
#  summarized_events = summarized_events %>%
#    ddply(.(customer_id), incorporate_events, membership)

  # summarized_events is .data
  # summarized_events.customer_id is .variables
  # incorporate_events is .fun
  # membership is passed to incorporate_events function (summarized_events goes in as s, being first argument)
  
  subset_summarized_events <- summarized_events %>% dplyr::filter(customer_id == 13865)
  
  subset_summarized_events <- summarized_events %>% dplyr::filter(customer_id == 12875)
  
  s <- subset_summarized_events
  membership_data <- membership
      
incorporate_events = function(s, membership_data) {
  m = membership_data %>% dplyr::filter(customer_id == s$customer_id[1])
  for(i in 1:nrow(s)) {
    # find row in membership data where stripe subscription id matches
    id_match = which(m$subscription_id == s$subscription_id[i])[ifelse(length(which(m$subscription_id == s$subscription_id[i]))>0,length(which(m$subscription_id == s$subscription_id[i])),1)]
    # if membership came before event data is tracked, then there should only be 1 row and we'll use that one (unless we're creating a new, unexisting membership)
    if(is.na(id_match) & (m$datestarted[1] < as.POSIXct('2016-04-09', tz = 'UTC') & !is.na(m$dateended[1])) & s$event_type[i] != 'customer.subscription.created' & nrow(m) > 0) {
      id_match = 1
      m[id_match,'subscription_id'] = s$subscription_id[i]
    }
    # for new record: either no customer id already in membership table, or a created event for a customer not prelaunch prestripe and without an already matching id (these subscriptions were added at later date),
    # or, if the membership was originally prelaunch,
    if((i == 1 & nrow(m) == 0) | (s$event_type[i] == 'customer.subscription.created' & (m$prelaunch_prestripe[1] == 0 | (!is.na(m$subscription_id[1]) & m$subscription_id[1] != s$subscription_id[i]))) |
       (is.na(id_match) & m$prelaunch_prestripe[1] != 0)) {
      num = nrow(m) + 1
      # if there is no member record already
      # initialize
      m[num,] = rep(NA, length(names(m)))
      # get customer id, email, subscription id, interval, and status from event object
      m[num,colnames(m) %in% colnames(s)] = s[i,colnames(s) %in% colnames(m)]
      # get started metro and current metro
      m[num,c('started_metro','current_metro')] = s$metro[i]
      # get trial
      m[num,'trial'] = ifelse(grepl('with Trial',s$membership_type[i]) | grepl('w/trial',s$membership_type[i]), 'yes', 'no')
      # get datestarted, dateended
      m[num,'datestarted'] = s$started_at[i]
      m[num,'dateended'] = s$ended_at[i]
      # set prelaunch_prestripe, to_change, from_change, recovered
      m[num,c('prelaunch_prestripe','to_change','from_change','recovered')] = 0
    } else if(s$event_type[i] == 'customer.subscription.created' & m$prelaunch_prestripe[1] == 1 & is.na(m$subscription_id[1])) { # populate missing stripe subscription ids for prelaunch prestripe
      if(s$interval[i] == m$interval[1]) m[1,'subscription_id'] = s[i,'subscription_id'] # set if interval matches
      else { #otherwise, just create a new row
        num = nrow(m) + 1
        # if there is no member record already
        # initialize
        m[num,] = rep(NA, length(names(m)))
        # get customer id, email, subscription id, interval, and status from event object
        m[num,colnames(m) %in% colnames(s)] = s[i,colnames(s) %in% colnames(m)]
        # get started metro and current metro
        m[num,c('started_metro','current_metro')] = s$metro[i]
        # get trial
        m[num,'trial'] = ifelse(grepl('with Trial',s$membership_type[i]) | grepl('w/trial',s$membership_type[i]), 'yes', 'no')
        # get datestarted, dateended
        m[num,'datestarted'] = s$started_at[i]
        m[num,'dateended'] = s$ended_at[i]
        # set prelaunch_prestripe, to_change, from_change, recovered
        m[num,c('prelaunch_prestripe','to_change','from_change','recovered')] = 0
      }
    } else if(s$event_type[i] != 'customer.subscription.created') {
      # if no id_match object, default to the first row
      if(is.na(id_match) & m$prelaunch_prestripe[1] == 0) { # non created event for prelaunch prestripe where no matching id
        id_match = 1
        m[id_match,'subscription_id'] = s$subscription_id[i]
      }
      if((m[id_match,'status'] == 'unpaid' | m[id_match,'status'] == 'canceled') & s[i,'status'] == 'active' & !is.na(m[id_match,'dateended'])) { #reactivated membership
        num = nrow(m) + 1
        m[num,] = m[id_match,]
        m[id_match,'subscription_id'] = paste0('was_',m[id_match,'subscription_id'])
        m[num,'datestarted'] = s[i,'time_stamp']
        m[num,'dateended'] = s[i,'ended_at']
        m[num,'prelaunch_prestripe'] = 0
        m[num,'recovered'] = 1
        id_match = num
      } else if(s$event_type[i] == 'customer.subscription.deleted' | ((s[i,'status'] == 'active') & m[id_match,'status'] == 'past_due')) { # canceled or active from past_due
        m[id_match,'dateended'] = s[i,'ended_at']
      } else if(s[i,'status'] == 'past_due') { # past_due
        m[id_match,'dateended'] = s[i,'time_stamp']
      }
      # get current status and metro from event object
      m[id_match,c('status','current_metro','datestarted')] = s[i,c('status','metro','started_at')]
      # get datestarted if not prelaunch prestripe
      if(m[id_match,'prelaunch_prestripe'] == 0) m[id_match,'datestarted'] = s[i,'datestarted']
    }
  }
  m = m[!duplicated(m),]
  m = rbind(m %>% dplyr::filter(!(duplicated(subscription_id) | duplicated(subscription_id, fromLast = T))), m %>% dplyr::filter(duplicated(subscription_id) | duplicated(subscription_id, fromLast = T)
  ) %>% arrange(factor(status, levels = c('canceled','unpaid','past_due','active','trialing'))) %>% dplyr::filter(!duplicated(subscription_id)))
  return(m)
}
  
  
  
  
  # incorporate stripe events
  summarized_events = stripe_events %>% 
    mutate(interval = ifelse(interval == 'month', 'monthly', 
                             ifelse(interval == 'year', 'annual', interval))) %>% 
    ddply(.(customer_id), incorporate_events, membership)

    # add in all membership data
  summarized_events = rbind(summarized_events, 
                            membership %>% 
                              dplyr::filter(!(customer_id %in% summarized_events$customer_id))) %>% 
                        arrange(customer_id)
  # add in prelaunch prestripe churn

consolidate_memberships = function(m) {
  if(nrow(m) > 1) {
    m = arrange(m, datestarted)
    # use first as baseline
    n = m[1,]
    l = 1
    # now iterate thru
    for(i in 2:nrow(m)) {
      # check if end date of earlier is before or after start date of later
      first_end = n$dateended[l]
      second_start = m$datestarted[i]
      second_end = m$dateended[i]
      # we do nothing if first end is NA and second end is not NA
      # case 1: first end and second end are NA, then keep first and move to second
      if(is.na(first_end) & is.na(second_end)) {
        l = l + 1
        n[l,] = m[i,]
      } else if(abs(first_end - second_start) < days(5) & n$interval[l] != m$interval[i] & !is.na(first_end)) { # case 2: upgrade/downgrade situation, make new row
        l = l + 1
        n[l,] = m[i,]
        n$to_change[l] = 1
        n$from_change[l-1] = 1
      } else if(first_end >= second_start & !is.na(first_end)) { # case 3: first end is after second start, then write some data from second to first
        n[l,c('subscription_id','interval','status','dateended','prelaunch_prestripe')] = m[i,c('subscription_id','interval','status','dateended','prelaunch_prestripe')]
      } else if(first_end < second_start & !is.na(first_end)) { # case 4: first end is before second start, then keep first and move to second
        l = l + 1
        n[l,] = m[i,]
      }
    }
    m = n
  }
  return(m)
}

# update from customers table for prelaunch prestripe
update_prelaunch_prestripe = function(summarized_events, guest_data) {
  prelaunch_churn = summarized_events %>% dplyr::filter(prelaunch_prestripe == 1, is.na(dateended), is.na(subscription_id)) %>% merge(guest_data, by = 'customer_id') %>%
    select(customer_id, prelaunch_prestripe, e = updated_at)
  summarized_events = summarized_events %>% merge(prelaunch_churn, all.x = T) %>% mutate(dateended = as.POSIXct(ifelse(!is.na(e),e,dateended),origin='1970-01-01')) %>% select(-e)
  return(summarized_events)
}

  
    summarized_events = update_prelaunch_prestripe(summarized_events, guest_data)
  #consolidate memberships to file that will be written
  membership_write = summarized_events %>% dplyr::filter(!(duplicated(customer_id) | duplicated(customer_id, fromLast = T))) %>% rbind(summarized_events %>% dplyr::filter(duplicated(customer_id) | duplicated(customer_id, fromLast = T)) %>%
                                                                                                                                         ddply(.(customer_id), consolidate_memberships)) %>% arrange(customer_id, datestarted) %>% dplyr::filter(!is.na(customer_id), interval %in% c('annual','monthly'))
  membership_write = membership_write[!duplicated(membership_write),]






# library('plyr')
# library('dplyr')
# library('lubridate')
# library('ggplot2')
# library('reshape2')
# library('scales')
# library('RPostgreSQL')
# library('caroline')
# 
# # function to get event data
# get_event_data = function(login, drv, date) {
#   prod = dbConnect(drv, dbname=login['PROD_RR_DB_SCHEMA'],host=login['PROD_RR_DB_HOST'],port=login['PROD_RR_DB_PORT'],user=login['PROD_RR_DB_USER'],password=login['PROD_RR_DB_PWD'])
#   data = dbGetQuery(prod, paste0("select billing_events.created_at time_stamp,
#                                  billing_events.customer_id customer_id,
#                                  customers.name as name,
#                                  customers.username as email,
#                                  customers.guest_account as guest,
#                                  billing_events.payload->>'customer' stripe_customer_id,
#                                  metros.name as metro,
#                                  billing_events.payload->>'id' subscription_id,
#                                  billing_events.event event_type,
#                                  billing_events.payload->>'start' started_at,
#                                  billing_events.payload->>'ended_at' ended_at,
#                                  billing_events.payload->'plan'->>'name' membership_type,
#                                  billing_events.payload->'plan'->>'interval' as interval,
#                                  billing_events.payload->>'status' status,
#                                  billing_events.payload->'discount'->'coupon'->>'id' coupon
#                                  from billing_events
#                                  left join customers on billing_events.customer_id = customers.id
#                                  left join metros on customers.metro_id = metros.id
#                                  where (event = 'customer.subscription.created' or event = 'customer.subscription.deleted' or event = 'customer.subscription.updated')
#                                  and billing_events.created_at::date = '", date, "'
#                                  order by 1"))
#   dbDisconnect(prod)
#   return(data)
# }
# 
# # function to get membership data
# get_membership_data = function(login, drv) {
#   proc = dbConnect(drv, dbname=login['DATA_DB_SCHEMA'],host=login['DATA_DB_HOST'],port=login['DATA_DB_PORT'],user=login['DATA_DB_USER'],password=login['DATA_DB_PWD'])
#   data = dbReadTable(proc, 'membership_current')
#   dbDisconnect(proc)
#   data = data %>% select(-id)
#   return(data)
# }
# 
# get_guest_data = function(login, drv) {
#   prod = dbConnect(drv, dbname=login['PROD_RR_DB_SCHEMA'],host=login['PROD_RR_DB_HOST'],port=login['PROD_RR_DB_PORT'],user=login['PROD_RR_DB_USER'],password=login['PROD_RR_DB_PWD'])
#   data = dbGetQuery(prod, 'select id customer_id, updated_at from customers where guest_account is true')
#   dbDisconnect(prod)
#   return(data)
# }
# 
# # incorporate event data
# incorporate_events = function(s, membership_data) {
#   m = membership_data %>% dplyr::filter(customer_id == s$customer_id[1])
#   for(i in 1:nrow(s)) {
#     # find row in membership data where stripe subscription id matches
#     id_match = which(m$subscription_id == s$subscription_id[i])[ifelse(length(which(m$subscription_id == s$subscription_id[i]))>0,length(which(m$subscription_id == s$subscription_id[i])),1)]
#     # if membership came before event data is tracked, then there should only be 1 row and we'll use that one (unless we're creating a new, unexisting membership)
#     if(is.na(id_match) & (m$datestarted[1] < as.POSIXct('2016-04-09', tz = 'UTC') & !is.na(m$dateended[1])) & s$event_type[i] != 'customer.subscription.created' & nrow(m) > 0) {
#       id_match = 1
#       m[id_match,'subscription_id'] = s$subscription_id[i]
#     }
#     # for new record: either no customer id already in membership table, or a created event for a customer not prelaunch prestripe and without an already matching id (these subscriptions were added at later date),
#     # or, if the membership was originally prelaunch,
#     if((i == 1 & nrow(m) == 0) | (s$event_type[i] == 'customer.subscription.created' & (m$prelaunch_prestripe[1] == 0 | (!is.na(m$subscription_id[1]) & m$subscription_id[1] != s$subscription_id[i]))) |
#        (is.na(id_match) & m$prelaunch_prestripe[1] != 0)) {
#       num = nrow(m) + 1
#       # if there is no member record already
#       # initialize
#       m[num,] = rep(NA, length(names(m)))
#       # get customer id, email, subscription id, interval, and status from event object
#       m[num,colnames(m) %in% colnames(s)] = s[i,colnames(s) %in% colnames(m)]
#       # get started metro and current metro
#       m[num,c('started_metro','current_metro')] = s$metro[i]
#       # get trial
#       m[num,'trial'] = ifelse(grepl('with Trial',s$membership_type[i]) | grepl('w/trial',s$membership_type[i]), 'yes', 'no')
#       # get datestarted, dateended
#       m[num,'datestarted'] = s$started_at[i]
#       m[num,'dateended'] = s$ended_at[i]
#       # set prelaunch_prestripe, to_change, from_change, recovered
#       m[num,c('prelaunch_prestripe','to_change','from_change','recovered')] = 0
#     } else if(s$event_type[i] == 'customer.subscription.created' & m$prelaunch_prestripe[1] == 1 & is.na(m$subscription_id[1])) { # populate missing stripe subscription ids for prelaunch prestripe
#       if(s$interval[i] == m$interval[1]) m[1,'subscription_id'] = s[i,'subscription_id'] # set if interval matches
#       else { #otherwise, just create a new row
#         num = nrow(m) + 1
#         # if there is no member record already
#         # initialize
#         m[num,] = rep(NA, length(names(m)))
#         # get customer id, email, subscription id, interval, and status from event object
#         m[num,colnames(m) %in% colnames(s)] = s[i,colnames(s) %in% colnames(m)]
#         # get started metro and current metro
#         m[num,c('started_metro','current_metro')] = s$metro[i]
#         # get trial
#         m[num,'trial'] = ifelse(grepl('with Trial',s$membership_type[i]) | grepl('w/trial',s$membership_type[i]), 'yes', 'no')
#         # get datestarted, dateended
#         m[num,'datestarted'] = s$started_at[i]
#         m[num,'dateended'] = s$ended_at[i]
#         # set prelaunch_prestripe, to_change, from_change, recovered
#         m[num,c('prelaunch_prestripe','to_change','from_change','recovered')] = 0
#       }
#     } else if(s$event_type[i] != 'customer.subscription.created') {
#       # if no id_match object, default to the first row
#       if(is.na(id_match) & m$prelaunch_prestripe[1] == 0) { # non created event for prelaunch prestripe where no matching id
#         id_match = 1
#         m[id_match,'subscription_id'] = s$subscription_id[i]
#       }
#       if((m[id_match,'status'] == 'unpaid' | m[id_match,'status'] == 'canceled') & s[i,'status'] == 'active' & !is.na(m[id_match,'dateended'])) { #reactivated membership
#         num = nrow(m) + 1
#         m[num,] = m[id_match,]
#         m[id_match,'subscription_id'] = paste0('was_',m[id_match,'subscription_id'])
#         m[num,'datestarted'] = s[i,'time_stamp']
#         m[num,'dateended'] = s[i,'ended_at']
#         m[num,'prelaunch_prestripe'] = 0
#         m[num,'recovered'] = 1
#         id_match = num
#       } else if(s$event_type[i] == 'customer.subscription.deleted' | ((s[i,'status'] == 'active') & m[id_match,'status'] == 'past_due')) { # canceled or active from past_due
#         m[id_match,'dateended'] = s[i,'ended_at']
#       } else if(s[i,'status'] == 'past_due') { # past_due
#         m[id_match,'dateended'] = s[i,'time_stamp']
#       }
#       # get current status and metro from event object
#       m[id_match,c('status','current_metro','datestarted')] = s[i,c('status','metro','started_at')]
#       # get datestarted if not prelaunch prestripe
#       if(m[id_match,'prelaunch_prestripe'] == 0) m[id_match,'datestarted'] = s[i,'datestarted']
#     }
#   }
#   m = m[!duplicated(m),]
#   m = rbind(m %>% dplyr::filter(!(duplicated(subscription_id) | duplicated(subscription_id, fromLast = T))), m %>% dplyr::filter(duplicated(subscription_id) | duplicated(subscription_id, fromLast = T)
#   ) %>% arrange(factor(status, levels = c('canceled','unpaid','past_due','active','trialing'))) %>% dplyr::filter(!duplicated(subscription_id)))
#   return(m)
# }
# 
# # consolidate memberships
# consolidate_memberships = function(m) {
#   if(nrow(m) > 1) {
#     m = arrange(m, datestarted)
#     # use first as baseline
#     n = m[1,]
#     l = 1
#     # now iterate thru
#     for(i in 2:nrow(m)) {
#       # check if end date of earlier is before or after start date of later
#       first_end = n$dateended[l]
#       second_start = m$datestarted[i]
#       second_end = m$dateended[i]
#       # we do nothing if first end is NA and second end is not NA
#       # case 1: first end and second end are NA, then keep first and move to second
#       if(is.na(first_end) & is.na(second_end)) {
#         l = l + 1
#         n[l,] = m[i,]
#       } else if(abs(first_end - second_start) < days(5) & n$interval[l] != m$interval[i] & !is.na(first_end)) { # case 2: upgrade/downgrade situation, make new row
#         l = l + 1
#         n[l,] = m[i,]
#         n$to_change[l] = 1
#         n$from_change[l-1] = 1
#       } else if(first_end >= second_start & !is.na(first_end)) { # case 3: first end is after second start, then write some data from second to first
#         n[l,c('subscription_id','interval','status','dateended','prelaunch_prestripe')] = m[i,c('subscription_id','interval','status','dateended','prelaunch_prestripe')]
#       } else if(first_end < second_start & !is.na(first_end)) { # case 4: first end is before second start, then keep first and move to second
#         l = l + 1
#         n[l,] = m[i,]
#       }
#     }
#     m = n
#   }
#   return(m)
# }
# 
# # update from customers table for prelaunch prestripe
# update_prelaunch_prestripe = function(summarized_events, guest_data) {
#   prelaunch_churn = summarized_events %>% dplyr::filter(prelaunch_prestripe == 1, is.na(dateended), is.na(subscription_id)) %>% merge(guest_data, by = 'customer_id') %>%
#     select(customer_id, prelaunch_prestripe, e = updated_at)
#   summarized_events = summarized_events %>% merge(prelaunch_churn, all.x = T) %>% mutate(dateended = as.POSIXct(ifelse(!is.na(e),e,dateended),origin='1970-01-01')) %>% select(-e)
#   return(summarized_events)
# }
# 
# # update DB table
# update_db = function(login, drv, data) {
#   proc = dbConnect(drv, dbname=login['DATA_DB_SCHEMA'],host=login['DATA_DB_HOST'],port=login['DATA_DB_PORT'],user=login['DATA_DB_USER'],password=login['DATA_DB_PWD'])
#   dbWriteTable2(proc, 'membership_current', data, row.names = F, overwrite = T, add.id = T)
#   dbDisconnect(proc)
# }
# 
# # store snapshot
# store_snapshot = function(login, drv, data, date) {
#   proc = dbConnect(drv, dbname=login['DATA_DB_SCHEMA'],host=login['DATA_DB_HOST'],port=login['DATA_DB_PORT'],user=login['DATA_DB_USER'],password=login['DATA_DB_PWD'])
#   dbSendQuery(proc, paste0("insert into membership_snapshots (snapshot_date, customer_id, email, subscription_id, started_metro, current_metro, interval, trial,
#                            status, datestarted, dateended, prelaunch_prestripe, to_change, from_change, recovered)
#                            (select '",date,"'::date, customer_id, email, subscription_id, started_metro, current_metro, interval, trial,
#                            status, datestarted, dateended, prelaunch_prestripe, to_change, from_change, recovered from membership_current)"))
#   dbDisconnect(proc)
#   
#   # rd = dbConnect(drv, dbname=login['SEGMENT_DB_NAME'],host=login['SEGMENT_DB_HOST'],port=login['SEGMENT_DB_PORT'],user=login['SEGMENT_DB_USER'],password=login['SEGMENT_DB_PWD'])
#   # # still need to add date
#   # dbWriteTable2(rd, 'membership_snapshots', data)
# }
# 
# #main method
# main = function() {
#   # accept command line arguments
#   args <- (commandArgs(trailingOnly = TRUE))
#   if(length(args)==0) {
#     # supply default value
#     date <- as.character(as_date(with_tz(Sys.time(), "GMT")))
#     print(paste0("No date parameter supplied, default to current date ", dt, " in UTC."))
#   } else{
#     date <- as.character(args[1])
#   }
#   
#   # posixct date
#   p_date = as.POSIXct(date, format = '%Y-%m-%d')
#   
#   # production db credentials
#   login_prod = Sys.getenv(c('PROD_RR_DB_HOST','PROD_RR_DB_SCHEMA','PROD_RR_DB_PORT','PROD_RR_DB_USER', 'PROD_RR_DB_PWD'))
#   drv = dbDriver("PostgreSQL")
#   # get event data
#   stripe_events = get_event_data(login_prod, drv, date)
#   stripe_events = stripe_events %>% mutate(started_at = as.POSIXct(as.numeric(started_at), origin = '1970-01-01'), ended_at = as.POSIXct(as.numeric(ended_at), origin = '1970-01-01'))
#   
#   # data sci db credentials
#   login_proc = Sys.getenv(c('DATA_DB_HOST','DATA_DB_USER','DATA_DB_PWD','DATA_DB_SCHEMA','DATA_DB_PORT'))
#   # get membership data
#   membership = get_membership_data(login_proc, drv)
#   # get guest data
#   guest_data = get_guest_data(login_prod, drv)
#   
#   # incorporate stripe events
#   summarized_events = stripe_events %>% mutate(interval = ifelse(interval == 'month', 'monthly', ifelse(interval == 'year', 'annual', interval))) %>% ddply(.(customer_id), incorporate_events, membership)
#   # add in all membership data
#   summarized_events = rbind(summarized_events, membership %>% dplyr::filter(!(customer_id %in% summarized_events$customer_id))) %>% arrange(customer_id)
#   # add in prelaunch prestripe churn
#   summarized_events = update_prelaunch_prestripe(summarized_events, guest_data)
#   #consolidate memberships to file that will be written
#   membership_write = summarized_events %>% dplyr::filter(!(duplicated(customer_id) | duplicated(customer_id, fromLast = T))) %>% rbind(summarized_events %>% dplyr::filter(duplicated(customer_id) | duplicated(customer_id, fromLast = T)) %>%
#                                                                                                                                          ddply(.(customer_id), consolidate_memberships)) %>% arrange(customer_id, datestarted) %>% dplyr::filter(!is.na(customer_id), interval %in% c('annual','monthly'))
#   membership_write = membership_write[!duplicated(membership_write),]
#   
#   # update db
#   update_db(login_proc, drv, membership_write)
#   
#   # if date is right, store snapshot in data db
#   if(day(p_date + days(1)) == 1 | day(p_date + days(1)) == 15) {
#     store_snapshot(login_proc, drv, membership_write, date)
#   }
#   
#   # if date is right, store snapshot in redshift
#   # if(day(pdate) == 1 | day(pdate) == 15) {
#   #   login_rd = Sys.getenv(c('SEGMENT_DB_HOST','SEGMENT_DB_NAME','SEGMENT_DB_PORT','SEGMENT_DB_USER', 'SEGMENT_DB_PWD'))
#   #   store_snapshot(login_rd, drv, membership_write)
#   # }
# }