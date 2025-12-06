---
navigation_title: "Wrapper"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wrapper-query.html
---

# Wrapper query [query-dsl-wrapper-query]


A query that accepts any other query as base64 encoded string.

```console
GET /_search
{
  "query": {
    "wrapper": {
      "query": "eyJ0ZXJtIiA6IHsgInVzZXIuaWQiIDogImtpbWNoeSIgfX0=" <1>
    }
  }
}
```

1. Base64 encoded string:  `{"term" : { "user.id" : "kimchy" }}`


This query is more useful in the context of Spring Data Elasticsearch. It’s the way a user can add custom queries when using Spring Data repositories. The user can add a @Query() annotation to a repository method. When such a method is called we do a parameter replacement in the query argument of the annotation and then send this as the query part of a search request.

