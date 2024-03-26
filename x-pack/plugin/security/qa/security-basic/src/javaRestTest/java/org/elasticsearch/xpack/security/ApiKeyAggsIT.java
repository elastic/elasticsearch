/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.HttpHeaders;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.QueryApiKeyIT.createApiKey;
import static org.elasticsearch.xpack.security.QueryApiKeyIT.createSystemWriteRole;
import static org.elasticsearch.xpack.security.QueryApiKeyIT.createUser;
import static org.elasticsearch.xpack.security.QueryApiKeyIT.grantApiKey;
import static org.elasticsearch.xpack.security.QueryApiKeyIT.invalidateApiKey;
import static org.elasticsearch.xpack.security.QueryApiKeyIT.updateApiKeys;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ApiKeyAggsIT extends SecurityInBasicRestTestCase {

    @SuppressWarnings("unchecked")
    public void testFiltersAggs() throws IOException {
        // admin keys
        createApiKey(
            "key1",
            Map.of("tags", List.of("prod", "est"), "label", "value1", "environment", Map.of("system", false, "hostname", "my-org-host-1")),
            API_KEY_ADMIN_AUTH_HEADER
        );
        createApiKey(
            "key2",
            Map.of("tags", List.of("prod", "west"), "label", "value2", "environment", Map.of("system", false, "hostname", "my-org-host-2")),
            API_KEY_ADMIN_AUTH_HEADER
        );
        createApiKey(
            "key3",
            Map.of("tags", List.of("prod", "south"), "label", "value3", "environment", Map.of("system", true, "hostname", "my-org-host-2")),
            API_KEY_ADMIN_AUTH_HEADER
        );
        // user keys
        createApiKey(
            "key4",
            Map.of("tags", List.of("prod", "north"), "label", "value4", "environment", Map.of("system", true, "hostname", "my-org-host-1")),
            API_KEY_USER_AUTH_HEADER
        );
        createApiKey(
            "wild",
            Map.of(
                "tags",
                List.of("staging", "west"),
                "label",
                "value5",
                "environment",
                Map.of("system", true, "hostname", "my-org-host-3")
            ),
            API_KEY_USER_AUTH_HEADER
        );
        assertAggs(API_KEY_ADMIN_AUTH_HEADER, """
            {
              "aggs": {
                "hostnames": {
                  "filters": {
                    "filters": {
                      "my-org-host-1": { "term": {"metadata.environment.hostname": "my-org-host-1"}},
                      "my-org-host-2": { "match": {"metadata": "my-org-host-2"}}
                    }
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((Map<String, Object>) ((Map<String, Object>) aggs.get("hostnames")).get("buckets")).size(), is(2));
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("hostnames")).get("buckets")).get(
                    "my-org-host-1"
                )).get("doc_count"),
                is(2)
            );
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("hostnames")).get("buckets")).get(
                    "my-org-host-2"
                )).get("doc_count"),
                is(2)
            );
        });
        // other bucket
        assertAggs(API_KEY_USER_AUTH_HEADER, """
            {
              "aggs": {
                "only_user_keys": {
                  "filters": {
                    "other_bucket_key": "other_user_keys",
                    "filters": {
                      "only_key4_match": { "bool": { "should": [{"prefix": {"name": "key"}}, {"match": {"metadata.tags": "prod"}}]}}
                    }
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((Map<String, Object>) ((Map<String, Object>) aggs.get("only_user_keys")).get("buckets")).size(), is(2));
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("only_user_keys")).get("buckets")).get(
                    "only_key4_match"
                )).get("doc_count"),
                is(1)
            );
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) aggs.get("only_user_keys")).get("buckets")).get(
                    "other_user_keys"
                )).get("doc_count"),
                is(1)
            );
        });
        // anonymous filters
        assertAggs(API_KEY_USER_AUTH_HEADER, """
            {
              "aggs": {
                "all_user_keys": {
                  "filters": {
                    "other_bucket_key": "other_user_keys",
                    "filters": [
                      {"match_all": {}},
                      {"exists": {"field": "username"}},
                      {"wildcard": {"name": {"value": "*"}}}
                    ]
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).size(), is(4));
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(0).get("doc_count"),
                is(2)
            );
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(1).get("doc_count"),
                is(2)
            );
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(2).get("doc_count"),
                is(2)
            );
            // the "other" bucket
            assertThat(
                ((List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_user_keys")).get("buckets")).get(3).get("doc_count"),
                is(0)
            );
        });
        // nested filters
        assertAggs(API_KEY_USER_AUTH_HEADER, """
            {
              "aggs": {
                "level1": {
                  "filters": {
                    "keyed": false,
                    "filters": {
                      "rest-filter": {"term": {"type": "rest"}},
                      "user-filter": {"wildcard": {"username": "api_*_user"}}
                    }
                  },
                  "aggs": {
                    "level2": {
                      "filters": {
                        "filters": {
                          "invalidated": {"term": {"invalidated": true}},
                          "not-invalidated": {"term": {"invalidated": false}}
                        }
                      }
                    }
                  }
                }
              }
            }
            """, aggs -> {
            List<Map<String, Object>> level1Buckets = (List<Map<String, Object>>) ((Map<String, Object>) aggs.get("level1")).get("buckets");
            assertThat(level1Buckets.size(), is(2));
            assertThat(level1Buckets.get(0).get("doc_count"), is(2));
            assertThat(level1Buckets.get(0).get("key"), is("rest-filter"));
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) level1Buckets.get(0).get("level2")).get("buckets"))
                    .get("invalidated")).get("doc_count"),
                is(0)
            );
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) level1Buckets.get(0).get("level2")).get("buckets"))
                    .get("not-invalidated")).get("doc_count"),
                is(2)
            );
            assertThat(level1Buckets.get(1).get("doc_count"), is(2));
            assertThat(level1Buckets.get(1).get("key"), is("user-filter"));
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) level1Buckets.get(1).get("level2")).get("buckets"))
                    .get("invalidated")).get("doc_count"),
                is(0)
            );
            assertThat(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) level1Buckets.get(1).get("level2")).get("buckets"))
                    .get("not-invalidated")).get("doc_count"),
                is(2)
            );
        });
        // filter on disallowed fields
        {
            Request request = new Request("GET", "/_security/_query/api_key");
            request.setOptions(
                request.getOptions()
                    .toBuilder()
                    .addHeader(HttpHeaders.AUTHORIZATION, randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER))
            );
            request.setJsonEntity("""
                {
                  "aggs": {
                    "wrong-field": {
                      "filters": {
                        "filters": {
                          "wrong-api-key-invalidated": { "term": {"api_key_invalidated": false}}
                        }
                      }
                    }
                  }
                }
                """);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(
                exception.getMessage(),
                containsString("Field [api_key_invalidated] is not allowed for API Key query or aggregation")
            );
        }
        {
            Request request = new Request("GET", "/_security/_query/api_key");
            request.setOptions(
                request.getOptions()
                    .toBuilder()
                    .addHeader(HttpHeaders.AUTHORIZATION, randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER))
            );
            request.setJsonEntity("""
                {
                  "aggs": {
                    "good-field": {
                      "filters": {
                        "filters": {
                          "good-api-key-invalidated": { "term": {"invalidated": false}}
                        }
                      },
                      "aggs": {
                        "wrong-field": {
                          "filters": {
                            "filters": {
                              "wrong-creator-realm": {"wildcard": {"creator.realm": "whatever"}}
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(exception.getMessage(), containsString("Field [creator.realm] is not allowed for API Key query or aggregation"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testAggsForType() throws IOException {
        List<String> crossApiKeyIds = new ArrayList<>();
        List<String> oldApiKeyIds = new ArrayList<>();
        List<String> otherApiKeyIds = new ArrayList<>();
        createApiKey("admin-rest-key", Map.of("tags", List.of("prod", "admin", "rest")), API_KEY_ADMIN_AUTH_HEADER);
        // this is to be mutated to look almost like a cross-cluster API key
        crossApiKeyIds.add(
            createApiKey("admin-cross-key", Map.of("tags", List.of("prod", "admin", "cross")), API_KEY_ADMIN_AUTH_HEADER).v1()
        );
        // this is to be mutated to look like an old API key with no type
        oldApiKeyIds.add(createApiKey("admin-old-key", Map.of("tags", List.of("prod", "admin", "old")), API_KEY_ADMIN_AUTH_HEADER).v1());
        // this is to be mutated to look like an unknown type of key
        otherApiKeyIds.add(
            createApiKey("admin-other-key", Map.of("tags", List.of("prod", "admin", "other")), API_KEY_ADMIN_AUTH_HEADER).v1()
        );

        createApiKey("user-rest-key", Map.of("tags", List.of("prod", "user", "rest")), API_KEY_USER_AUTH_HEADER);
        // this is to be mutated to look almost like a cross-cluster API key
        crossApiKeyIds.add(createApiKey("user-cross-key", Map.of("tags", List.of("prod", "user", "cross")), API_KEY_USER_AUTH_HEADER).v1());
        // this is to be mutated to look like an old API key with no type
        oldApiKeyIds.add(createApiKey("user-old-key", Map.of("tags", List.of("prod", "user", "old")), API_KEY_USER_AUTH_HEADER).v1());
        // this is to be mutated to look like an unknown type of key
        otherApiKeyIds.add(createApiKey("user-other-key", Map.of("tags", List.of("prod", "user", "other")), API_KEY_USER_AUTH_HEADER).v1());

        createSystemWriteRole("system_write");
        String systemWriteCreds = createUser("superuser_with_system_write", new String[] { "superuser", "system_write" });

        // test keys with no "type" field are still considered of type "rest"
        // this is so in order to accommodate pre-8.9 API keys which where all of type "rest" implicitly
        updateApiKeys(systemWriteCreds, "ctx._source.remove('type');", oldApiKeyIds);
        updateApiKeys(systemWriteCreds, "ctx._source['type']='other';", otherApiKeyIds);
        // hack to make these look like cross_cluster keys, because really creating those requires special setup and different license level
        updateApiKeys(systemWriteCreds, "ctx._source['type']='cross_cluster';", crossApiKeyIds);

        boolean isAdmin = randomBoolean();
        assertAggs(isAdmin ? API_KEY_ADMIN_AUTH_HEADER : API_KEY_USER_AUTH_HEADER, """
            {
              "size": 0,
              "aggs": {
                "all_keys_by_type": {
                  "composite": {
                    "sources": [
                      { "type": { "terms": { "field": "type" } } }
                    ]
                  }
                }
              }
            }
            """, aggs -> {
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) ((Map<String, Object>) aggs.get("all_keys_by_type")).get(
                "buckets"
            );
            assertThat(buckets.size(), is(3));
            assertThat(((Map<String, Object>) buckets.get(0).get("key")).get("type"), is("cross_cluster"));
            assertThat(((Map<String, Object>) buckets.get(1).get("key")).get("type"), is("other"));
            assertThat(((Map<String, Object>) buckets.get(2).get("key")).get("type"), is("rest"));
            if (isAdmin) {
                assertThat(buckets.get(0).get("doc_count"), is(2));
                assertThat(buckets.get(1).get("doc_count"), is(2));
                assertThat(buckets.get(2).get("doc_count"), is(4)); // 2 + 2 old ones with no explicit type set
            } else {
                assertThat(buckets.get(0).get("doc_count"), is(1));
                assertThat(buckets.get(1).get("doc_count"), is(1));
                assertThat(buckets.get(2).get("doc_count"), is(2)); // 1 + 1 old ones with no explicit type set
            }
        });

        assertAggs(isAdmin ? API_KEY_ADMIN_AUTH_HEADER : API_KEY_USER_AUTH_HEADER, """
            {
              "size": 0,
              "aggs": {
                "type_cardinality": {
                  "cardinality": {
                    "field": "type"
                  }
                },
                "type_value_count": {
                  "value_count": {
                    "field": "type"
                  }
                },
                "missing_type_count": {
                  "missing": {
                    "field": "type"
                  }
                },
                "type_terms": {
                  "terms": {
                    "field": "type"
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(aggs.size(), is(4));
            // 3 types
            assertThat(((Map<String, Object>) aggs.get("type_cardinality")).get("value"), is(3));
            if (isAdmin) {
                // 8 keys
                assertThat(((Map<String, Object>) aggs.get("type_value_count")).get("value"), is(8));
            } else {
                // 4 keys
                assertThat(((Map<String, Object>) aggs.get("type_value_count")).get("value"), is(4));
            }
            assertThat(((Map<String, Object>) aggs.get("missing_type_count")).get("doc_count"), is(0));
            List<Map<String, Object>> typeTermsBuckets = (List<Map<String, Object>>) ((Map<String, Object>) aggs.get("type_terms")).get(
                "buckets"
            );
            assertThat(typeTermsBuckets.size(), is(3));
        });
        // runtime type field is disallowed
        {
            Request request = new Request("GET", "/_security/_query/api_key");
            request.setOptions(
                request.getOptions()
                    .toBuilder()
                    .addHeader(HttpHeaders.AUTHORIZATION, randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER))
            );
            request.setJsonEntity("""
                {
                  "aggs": {
                    "type_value_count": {
                      "value_count": {
                        "field": "runtime_key_type"
                      }
                    }
                  }
                }
                """);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(exception.getMessage(), containsString("Field [runtime_key_type] is not allowed for API Key query or aggregation"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testFilterAggs() throws IOException {
        String user1Creds = createUser("test-user-1", new String[] { "api_key_user_role" });
        String user2Creds = createUser("test-user-2", new String[] { "api_key_user_role" });
        String user3Creds = createUser("test-user-3", new String[] { "api_key_user_role" });
        // create 6 keys for 3 users (2 keys each, one granted)
        grantApiKey("key-1-user-1", "10d", Map.of("labels", List.of("grant", "1", "10d")), API_KEY_ADMIN_AUTH_HEADER, "test-user-1").v1();
        String key2User1KeyId = createApiKey("key-2-user-1", "20d", null, Map.of("labels", List.of("2", "20d")), user1Creds).v1();
        grantApiKey("key-1-user-2", "30d", Map.of("labels", List.of("grant", "1", "30d")), API_KEY_ADMIN_AUTH_HEADER, "test-user-2").v1();
        createApiKey("key-2-user-2", "40d", null, Map.of("labels", List.of("2", "40d")), user2Creds).v1();
        String key1User3KeyId = grantApiKey(
            "key-1-user-3",
            "50d",
            Map.of("labels", List.of("grant", "1", "50d")),
            API_KEY_ADMIN_AUTH_HEADER,
            "test-user-3"
        ).v1();
        createApiKey("key-2-user-3", "60d", null, Map.of("labels", List.of("2", "60d")), user3Creds).v1();
        // invalidate some two keys
        invalidateApiKey(key2User1KeyId, false, API_KEY_ADMIN_AUTH_HEADER);
        invalidateApiKey(key1User3KeyId, false, API_KEY_ADMIN_AUTH_HEADER);

        assertAggs(API_KEY_ADMIN_AUTH_HEADER, """
            {
              "size": 0,
              "aggs": {
                "not_invalidated": {
                  "filter": { "term": { "invalidated": false } },
                  "aggs": {
                    "keys_by_username": {
                      "composite": {
                        "sources": [
                          { "usernames": { "terms": { "field": "username" } } }
                        ]
                      }
                    }
                  }
                }
              }
            }
            """, aggs -> {
            assertThat(((Map<String, Object>) aggs.get("not_invalidated")).get("doc_count"), is(4)); // 6 - 2 (invalidated)
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) ((Map<String, Object>) ((Map<String, Object>) aggs.get(
                "not_invalidated"
            )).get("keys_by_username")).get("buckets");
            assertThat(buckets.size(), is(3));
            assertThat(((Map<String, Object>) buckets.get(0).get("key")).get("usernames"), is("test-user-1"));
            assertThat(buckets.get(0).get("doc_count"), is(1));
            assertThat(((Map<String, Object>) buckets.get(1).get("key")).get("usernames"), is("test-user-2"));
            assertThat(buckets.get(1).get("doc_count"), is(2));
            assertThat(((Map<String, Object>) buckets.get(2).get("key")).get("usernames"), is("test-user-3"));
            assertThat(buckets.get(2).get("doc_count"), is(1));
        });

        assertAggs(API_KEY_ADMIN_AUTH_HEADER, """
            {
              "aggs": {
                "keys_by_username": {
                  "composite": {
                    "sources": [
                      { "usernames": { "terms": { "field": "username" } } }
                    ]
                  },
                  "aggs": {
                    "not_expired": {
                      "filter": {
                        "range": {
                          "expiration": {
                            "gte": "now+35d/d"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """, aggs -> {
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) ((Map<String, Object>) aggs.get("keys_by_username")).get(
                "buckets"
            );
            assertThat(buckets.size(), is(3));
            assertThat(buckets.get(0).get("doc_count"), is(2));
            assertThat(((Map<String, Object>) buckets.get(0).get("key")).get("usernames"), is("test-user-1"));
            assertThat(((Map<String, Object>) buckets.get(0).get("not_expired")).get("doc_count"), is(0));
            assertThat(buckets.get(1).get("doc_count"), is(2));
            assertThat(((Map<String, Object>) buckets.get(1).get("key")).get("usernames"), is("test-user-2"));
            assertThat(((Map<String, Object>) buckets.get(1).get("not_expired")).get("doc_count"), is(1));
            assertThat(buckets.get(2).get("doc_count"), is(2));
            assertThat(((Map<String, Object>) buckets.get(2).get("key")).get("usernames"), is("test-user-3"));
            assertThat(((Map<String, Object>) buckets.get(2).get("not_expired")).get("doc_count"), is(2));
        });
        // "creator" field is disallowed
        {
            Request request = new Request("GET", "/_security/_query/api_key");
            request.setOptions(
                request.getOptions()
                    .toBuilder()
                    .addHeader(HttpHeaders.AUTHORIZATION, randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER))
            );
            request.setJsonEntity("""
                {
                  "aggs": {
                    "keys_by_username": {
                      "composite": {
                        "sources": [
                          { "usernames": { "terms": { "field": "username" } } },
                          { "histo": { "histogram": { "field": "creator", "interval": 5 } } }
                        ]
                      }
                    }
                  }
                }
                """);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(exception.getMessage(), containsString("Field [creator] is not allowed for API Key query or aggregation"));
        }
    }

    public void testDisallowedAggTypes() {
        // global aggregation type MUST never be allowed in order to not expose non-owned non-API key docs
        {
            Request request = new Request("GET", "/_security/_query/api_key");
            request.setOptions(
                request.getOptions()
                    .toBuilder()
                    .addHeader(HttpHeaders.AUTHORIZATION, randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER))
            );
            request.setJsonEntity("""
                {
                  "aggs": {
                    "all_.security_docs": {
                      "global": {},
                      "aggs": {
                        "key_names": {
                          "terms": { "field": "name" }
                        }
                      }
                    }
                  }
                }
                """);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(exception.getMessage(), containsString("Unsupported API Keys agg [all_.security_docs] of type [global]"));
        }
        // pipeline aggs are not allowed but could be if there's an identified use-case
        {
            Request request = new Request("GET", "/_security/_query/api_key");
            request.setOptions(
                request.getOptions()
                    .toBuilder()
                    .addHeader(HttpHeaders.AUTHORIZATION, randomFrom(API_KEY_ADMIN_AUTH_HEADER, API_KEY_USER_AUTH_HEADER))
            );
            request.setJsonEntity("""
                {
                  "aggs": {
                    "type_cardinality": {
                      "cardinality": {
                        "field": "type"
                      }
                    },
                    "total_type_cardinality": {
                      "cumulative_cardinality": {
                        "buckets_path": "type_cardinality"
                      }
                    }
                  }
                }
                """);
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(exception.getResponse().toString(), exception.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(exception.getMessage(), containsString("Unsupported aggregation of type [cumulative_cardinality]"));
        }
    }

    void assertAggs(String authHeader, String body, Consumer<Map<String, Object>> aggsVerifier) throws IOException {
        final Request request = new Request("GET", "/_security/_query/api_key");
        request.setJsonEntity(body);
        request.setOptions(request.getOptions().toBuilder().addHeader(HttpHeaders.AUTHORIZATION, authHeader));
        final Response response = client().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);
        @SuppressWarnings("unchecked")
        final Map<String, Object> aggs = (Map<String, Object>) responseMap.get("aggregations");
        aggsVerifier.accept(aggs);
    }
}
