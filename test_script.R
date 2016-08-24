## Author: Vinay Bhat
## Date: 23 Aug 2016

print("this is a test script")

prod = dbConnect(drv, dbname=login['PROD_RR_DB_SCHEMA'],host=login['PROD_RR_DB_HOST'],port=login['PROD_RR_DB_PORT'],user=login['PROD_RR_DB_USER'],password=login['PROD_RR_DB_PWD'])
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
                                 and customer_id IN (13865)
                                 order by 1"))
dbDisconnect(prod)
