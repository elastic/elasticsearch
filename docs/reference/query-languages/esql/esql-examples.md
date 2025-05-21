---
navigation_title: "Examples"
---

# {{esql}} examples [esql-examples]

## Aggregating and enriching windows event logs

```esql
FROM logs-*
| WHERE event.code IS NOT NULL
| STATS event_code_count = COUNT(event.code) BY event.code,host.name
| ENRICH win_events ON event.code WITH event_description
| WHERE event_description IS NOT NULL and host.name IS NOT NULL
| RENAME event_description AS event.description
| SORT event_code_count DESC
| KEEP event_code_count,event.code,host.name,event.description
```

* It starts by querying logs from indices that match the pattern "logs-*".
* Filters events where the "event.code" field is not null.
* Aggregates the count of events by "event.code" and "host.name."
* Enriches the events with additional information using the "EVENT_DESCRIPTION" field.
* Filters out events where "EVENT_DESCRIPTION" or "host.name" is null.
* Renames "EVENT_DESCRIPTION" as "event.description."
* Sorts the result by "event_code_count" in descending order.
* Keeps only selected fields: "event_code_count," "event.code," "host.name," and "event.description."


## Summing outbound traffic from a process `curl.exe`

```esql
FROM logs-endpoint
| WHERE process.name == "curl.exe"
| STATS bytes = SUM(destination.bytes) BY destination.address
| EVAL kb =  bytes/1024
| SORT kb DESC
| LIMIT 10
| KEEP kb,destination.address
```

* Queries logs from the "logs-endpoint" source.
* Filters events where the "process.name" field is "curl.exe."
* Calculates the sum of bytes sent to destination addresses and converts it to kilobytes (KB).
* Sorts the results by "kb" (kilobytes) in descending order.
* Limits the output to the top 10 results.
* Keeps only the "kb" and "destination.address" fields.



## Manipulating DNS logs to find a high number of unique dns queries per registered domain

```esql
FROM logs-*
| GROK dns.question.name "%{DATA}\\.%{GREEDYDATA:dns.question.registered_domain:string}"
| STATS unique_queries = COUNT_DISTINCT(dns.question.name) BY dns.question.registered_domain, process.name
| WHERE unique_queries > 10
| SORT unique_queries DESC
| RENAME unique_queries AS `Unique Queries`, dns.question.registered_domain AS `Registered Domain`, process.name AS `Process`
```

* Queries logs from indices matching "logs-*."
* Uses the "grok" pattern to extract the registered domain from the "dns.question.name" field.
* Calculates the count of unique DNS queries per registered domain and process name.
* Filters results where "unique_queries" are greater than 10.
* Sorts the results by "unique_queries" in descending order.
* Renames fields for clarity: "unique_queries" to "Unique Queries," "dns.question.registered_domain" to "Registered Domain," and "process.name" to "Process."



## Identifying high-numbers of outbound user connections

```esql
FROM logs-*
| WHERE NOT CIDR_MATCH(destination.ip, "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
| STATS destcount = COUNT(destination.ip) BY user.name, host.name
| ENRICH ldap_lookup_new ON user.name
| WHERE group.name IS NOT NULL
| EVAL follow_up = CASE(destcount >= 100, "true","false")
| SORT destcount DESC
| KEEP destcount, host.name, user.name, group.name, follow_up
```

* Queries logs from indices matching "logs-*."
* Filters out events where the destination IP address falls within private IP address ranges (e.g., 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16).
* Calculates the count of unique destination IPs by "user.name" and "host.name."
* Enriches the "user.name" field with LDAP group information.
* Filters out results where "group.name" is not null.
* Uses a "CASE" statement to create a "follow_up" field, setting it to "true" when "destcount" is greater than or equal to 100 and "false" otherwise.
* Sorts the results by "destcount" in descending order.
* Keeps selected fields: "destcount," "host.name," "user.name," "group.name," and "follow_up."
