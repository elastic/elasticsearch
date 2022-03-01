/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.oldrepos;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class OldMappingsIT extends ESRestTestCase {

    static final Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));

    static boolean setupDone;

    static final String fileBeat5Mapping = """
        "_default_": {
              "_meta": {
                "version": "5.6.17"
              },
              "date_detection": false,
              "dynamic_templates": [
                {
                  "strings_as_keyword": {
                    "mapping": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "match_mapping_type": "string"
                  }
                }
              ],
              "properties": {
                "@timestamp": {
                  "type": "date"
                },
                "apache2": {
                  "properties": {
                    "access": {
                      "properties": {
                        "agent": {
                          "norms": false,
                          "type": "text"
                        },
                        "body_sent": {
                          "properties": {
                            "bytes": {
                              "type": "long"
                            }
                          }
                        },
                        "geoip": {
                          "properties": {
                            "city_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "continent_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "country_iso_code": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "location": {
                              "type": "geo_point"
                            },
                            "region_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "http_version": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "method": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "referrer": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "remote_ip": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "response_code": {
                          "type": "long"
                        },
                        "url": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "user_agent": {
                          "properties": {
                            "device": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "major": {
                              "type": "long"
                            },
                            "minor": {
                              "type": "long"
                            },
                            "name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "os": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "os_major": {
                              "type": "long"
                            },
                            "os_minor": {
                              "type": "long"
                            },
                            "os_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "patch": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "user_name": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    },
                    "error": {
                      "properties": {
                        "client": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "level": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "message": {
                          "norms": false,
                          "type": "text"
                        },
                        "module": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "pid": {
                          "type": "long"
                        },
                        "tid": {
                          "type": "long"
                        }
                      }
                    }
                  }
                },
                "auditd": {
                  "properties": {
                    "log": {
                      "properties": {
                        "a0": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "acct": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "geoip": {
                          "properties": {
                            "city_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "continent_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "country_iso_code": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "location": {
                              "type": "geo_point"
                            },
                            "region_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "item": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "items": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "new_auid": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "new_ses": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "old_auid": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "old_ses": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "pid": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "ppid": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "record_type": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "res": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "sequence": {
                          "type": "long"
                        }
                      }
                    }
                  }
                },
                "beat": {
                  "properties": {
                    "hostname": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "name": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "version": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    }
                  }
                },
                "error": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "fileset": {
                  "properties": {
                    "module": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "name": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    }
                  }
                },
                "input_type": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "message": {
                  "norms": false,
                  "type": "text"
                },
                "meta": {
                  "properties": {
                    "cloud": {
                      "properties": {
                        "availability_zone": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "instance_id": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "machine_type": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "project_id": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "provider": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "region": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    }
                  }
                },
                "mysql": {
                  "properties": {
                    "error": {
                      "properties": {
                        "level": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "message": {
                          "norms": false,
                          "type": "text"
                        },
                        "thread_id": {
                          "type": "long"
                        },
                        "timestamp": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    },
                    "slowlog": {
                      "properties": {
                        "host": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "id": {
                          "type": "long"
                        },
                        "ip": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "lock_time": {
                          "properties": {
                            "sec": {
                              "type": "float"
                            }
                          }
                        },
                        "query": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "query_time": {
                          "properties": {
                            "sec": {
                              "type": "float"
                            }
                          }
                        },
                        "rows_examined": {
                          "type": "long"
                        },
                        "rows_sent": {
                          "type": "long"
                        },
                        "timestamp": {
                          "type": "long"
                        },
                        "user": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    }
                  }
                },
                "nginx": {
                  "properties": {
                    "access": {
                      "properties": {
                        "agent": {
                          "norms": false,
                          "type": "text"
                        },
                        "body_sent": {
                          "properties": {
                            "bytes": {
                              "type": "long"
                            }
                          }
                        },
                        "geoip": {
                          "properties": {
                            "city_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "continent_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "country_iso_code": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "location": {
                              "type": "geo_point"
                            },
                            "region_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "http_version": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "method": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "referrer": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "remote_ip": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "response_code": {
                          "type": "long"
                        },
                        "url": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "user_agent": {
                          "properties": {
                            "device": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "major": {
                              "type": "long"
                            },
                            "minor": {
                              "type": "long"
                            },
                            "name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "os": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "os_major": {
                              "type": "long"
                            },
                            "os_minor": {
                              "type": "long"
                            },
                            "os_name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "patch": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "user_name": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    },
                    "error": {
                      "properties": {
                        "connection_id": {
                          "type": "long"
                        },
                        "level": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "message": {
                          "norms": false,
                          "type": "text"
                        },
                        "pid": {
                          "type": "long"
                        },
                        "tid": {
                          "type": "long"
                        }
                      }
                    }
                  }
                },
                "offset": {
                  "type": "long"
                },
                "read_timestamp": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "source": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "system": {
                  "properties": {
                    "auth": {
                      "properties": {
                        "groupadd": {
                          "properties": {
                            "gid": {
                              "type": "long"
                            },
                            "name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "hostname": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "message": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "pid": {
                          "type": "long"
                        },
                        "program": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "ssh": {
                          "properties": {
                            "dropped_ip": {
                              "type": "ip"
                            },
                            "event": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "geoip": {
                              "properties": {
                                "city_name": {
                                  "ignore_above": 1024,
                                  "type": "keyword"
                                },
                                "continent_name": {
                                  "ignore_above": 1024,
                                  "type": "keyword"
                                },
                                "country_iso_code": {
                                  "ignore_above": 1024,
                                  "type": "keyword"
                                },
                                "location": {
                                  "type": "geo_point"
                                },
                                "region_name": {
                                  "ignore_above": 1024,
                                  "type": "keyword"
                                }
                              }
                            },
                            "ip": {
                              "type": "ip"
                            },
                            "method": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "port": {
                              "type": "long"
                            },
                            "signature": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "sudo": {
                          "properties": {
                            "command": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "error": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "pwd": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "tty": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "user": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            }
                          }
                        },
                        "timestamp": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "user": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "useradd": {
                          "properties": {
                            "gid": {
                              "type": "long"
                            },
                            "home": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "name": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "shell": {
                              "ignore_above": 1024,
                              "type": "keyword"
                            },
                            "uid": {
                              "type": "long"
                            }
                          }
                        }
                      }
                    },
                    "syslog": {
                      "properties": {
                        "hostname": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "message": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "pid": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "program": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        },
                        "timestamp": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    }
                  }
                },
                "tags": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "type": {
                  "ignore_above": 1024,
                  "type": "keyword"
                }
              }
            }
        """;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void setupIndex() throws IOException {
        final boolean afterRestart = Booleans.parseBoolean(System.getProperty("tests.after_restart"));
        if (afterRestart) {
            return;
        }

        // The following is bit of a hack. While we wish we could make this an @BeforeClass, it does not work because the client() is only
        // initialized later, so we do it when running the first test
        if (setupDone) {
            return;
        }

        setupDone = true;

        String repoLocation = PathUtils.get(System.getProperty("tests.repo.location"))
            .resolve(RandomizedTest.getContext().getTargetClass().getName())
            .toString();

        String indexName = "filebeat5";
        String repoName = "old_mappings_repo";
        String snapshotName = "snap";

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            Request createIndex = new Request("PUT", "/" + indexName);
            int numberOfShards = randomIntBetween(1, 3);

            XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("settings")
                .field("index.number_of_shards", numberOfShards)
                .endObject()
                .startObject("mappings");
            builder.rawValue(new BytesArray(fileBeat5Mapping).streamInput(), XContentType.JSON);
            builder.endObject().endObject();

            createIndex.setJsonEntity(Strings.toString(builder));
            assertOK(oldEs.performRequest(createIndex));

            Request doc1 = new Request("PUT", "/" + indexName + "/" + "doc" + "/" + "1");
            doc1.addParameter("refresh", "true");
            XContentBuilder bodyDoc1 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl1")
                .endObject()
                .endObject()
                .endObject();
            doc1.setJsonEntity(Strings.toString(bodyDoc1));
            assertOK(oldEs.performRequest(doc1));

            Request doc2 = new Request("PUT", "/" + indexName + "/" + "doc" + "/" + "2");
            doc2.addParameter("refresh", "true");
            XContentBuilder bodyDoc2 = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("apache2")
                .startObject("access")
                .field("url", "myurl2")
                .endObject()
                .endObject()
                .endObject();
            doc2.setJsonEntity(Strings.toString(bodyDoc2));
            assertOK(oldEs.performRequest(doc2));

            // register repo on old ES and take snapshot
            Request createRepoRequest = new Request("PUT", "/_snapshot/" + repoName);
            createRepoRequest.setJsonEntity("""
                {"type":"fs","settings":{"location":"%s"}}
                """.formatted(repoLocation));
            assertOK(oldEs.performRequest(createRepoRequest));

            Request createSnapshotRequest = new Request("PUT", "/_snapshot/" + repoName + "/" + snapshotName);
            createSnapshotRequest.addParameter("wait_for_completion", "true");
            createSnapshotRequest.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
            assertOK(oldEs.performRequest(createSnapshotRequest));
        }

        // register repo on new ES and restore snapshot
        Request createRepoRequest2 = new Request("PUT", "/_snapshot/" + repoName);
        createRepoRequest2.setJsonEntity("""
            {"type":"fs","settings":{"location":"%s"}}
            """.formatted(repoLocation));
        assertOK(client().performRequest(createRepoRequest2));

        final Request createRestoreRequest = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        createRestoreRequest.addParameter("wait_for_completion", "true");
        createRestoreRequest.setJsonEntity("{\"indices\":\"" + indexName + "\"}");
        assertOK(client().performRequest(createRestoreRequest));
    }

    public void testSearchKeyword() throws IOException {
        Request search = new Request("POST", "/" + "filebeat5" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.url")
            .field("query", "myurl2")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        Map<String, Object> response = entityAsMap(client().performRequest(search));
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", response));
        assertThat(hits, hasSize(1));
        // Map<?, ?> bestHit = (Map<?, ?>) ().get(0);
        // List<?> date = (List<?>) XContentMapValues.extractValue("fields.date", bestHit);
        // assertThat(date.size(), equalTo(1));
    }

    public void testSearchOnPlaceHolderField() throws IOException {
        Request search = new Request("POST", "/" + "filebeat5" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("query")
            .startObject("match")
            .startObject("apache2.access.agent")
            .field("query", "some-agent")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        ResponseException re = expectThrows(ResponseException.class, () -> entityAsMap(client().performRequest(search)));
        assertThat(
            re.getMessage(),
            containsString("Field [apache2.access.agent] of type [text] in legacy index does not support match queries")
        );
    }

    public void testAggregationOnPlaceholderField() throws IOException {
        Request search = new Request("POST", "/" + "filebeat5" + "/_search");
        XContentBuilder query = XContentBuilder.builder(XContentType.JSON.xContent())
            .startObject()
            .startObject("aggs")
            .startObject("agents")
            .startObject("terms")
            .field("field", "apache2.access.agent")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        search.setJsonEntity(Strings.toString(query));
        ResponseException re = expectThrows(ResponseException.class, () -> entityAsMap(client().performRequest(search)));
        assertThat(re.getMessage(), containsString("can't run aggregation or sorts on field type text of legacy index"));
    }

}
