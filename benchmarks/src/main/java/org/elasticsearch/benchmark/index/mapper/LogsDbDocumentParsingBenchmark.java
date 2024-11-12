/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class LogsDbDocumentParsingBenchmark {
    private Random random;
    private MapperService mapperServiceEnabled;
    private MapperService mapperServiceEnabledWithStoreArrays;
    private MapperService mapperServiceDisabled;
    private SourceToParse[] documents;

    static {
        LogConfigurator.configureESLogging(); // doc values implementations need logging
    }

    private static String SAMPLE_LOGS_MAPPING_ENABLED = """
        {
          "_source": {
            "mode": "synthetic"
          },
          "properties": {
            "kafka": {
              "properties": {
                "log": {
                  "properties": {
                    "component": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "trace": {
                      "properties": {
                        "message": {
                          "type": "text"
                        },
                        "class": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    },
                    "thread": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "class": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    }
                  }
                }
              }
            },
            "host": {
              "properties": {
                "hostname": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "os": {
                  "properties": {
                    "build": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "kernel": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "codename": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "name": {
                      "ignore_above": 1024,
                      "type": "keyword",
                      "fields": {
                        "text": {
                          "type": "text"
                        }
                      }
                    },
                    "family": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "version": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "platform": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    }
                  }
                },
                "domain": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "ip": {
                  "type": "ip"
                },
                "containerized": {
                  "type": "boolean"
                },
                "name": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "id": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "type": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "mac": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "architecture": {
                  "ignore_above": 1024,
                  "type": "keyword"
                }
              }
            }
          }
        }
        """;

    private static String SAMPLE_LOGS_MAPPING_ENABLED_WITH_STORE_ARRAYS = """
        {
          "_source": {
            "mode": "synthetic"
          },
          "properties": {
            "kafka": {
              "properties": {
                "log": {
                  "properties": {
                    "component": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "trace": {
                      "synthetic_source_keep": "arrays",
                      "properties": {
                        "message": {
                          "type": "text"
                        },
                        "class": {
                          "ignore_above": 1024,
                          "type": "keyword"
                        }
                      }
                    },
                    "thread": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "class": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    }
                  }
                }
              }
            },
            "host": {
              "properties": {
                "hostname": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "os": {
                  "properties": {
                    "build": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "kernel": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "codename": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "name": {
                      "ignore_above": 1024,
                      "type": "keyword",
                      "fields": {
                        "text": {
                          "type": "text"
                        }
                      }
                    },
                    "family": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "version": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    },
                    "platform": {
                      "ignore_above": 1024,
                      "type": "keyword"
                    }
                  }
                },
                "domain": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "ip": {
                  "type": "ip"
                },
                "containerized": {
                  "type": "boolean"
                },
                "name": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "id": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "type": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "mac": {
                  "ignore_above": 1024,
                  "type": "keyword"
                },
                "architecture": {
                  "ignore_above": 1024,
                  "type": "keyword"
                }
              }
            }
          }
        }
        """;

    private static String SAMPLE_LOGS_MAPPING_DISABLED = """
        {
          "_source": {
            "mode": "synthetic"
          },
          "enabled": false
        }
        """;

    @Setup
    public void setUp() throws IOException {
        this.random = new Random();
        this.mapperServiceEnabled = MapperServiceFactory.create(SAMPLE_LOGS_MAPPING_ENABLED);
        this.mapperServiceEnabledWithStoreArrays = MapperServiceFactory.create(SAMPLE_LOGS_MAPPING_ENABLED_WITH_STORE_ARRAYS);
        this.mapperServiceDisabled = MapperServiceFactory.create(SAMPLE_LOGS_MAPPING_DISABLED);
        this.documents = generateRandomDocuments(10_000);
    }

    @Benchmark
    public List<LuceneDocument> benchmarkEnabledObject() {
        return mapperServiceEnabled.documentMapper().parse(randomFrom(documents)).docs();
    }

    @Benchmark
    public List<LuceneDocument> benchmarkEnabledObjectWithStoreArrays() {
        return mapperServiceEnabledWithStoreArrays.documentMapper().parse(randomFrom(documents)).docs();
    }

    @Benchmark
    public List<LuceneDocument> benchmarkDisabledObject() {
        return mapperServiceDisabled.documentMapper().parse(randomFrom(documents)).docs();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private <T> T randomFrom(T... items) {
        return items[random.nextInt(items.length)];
    }

    private SourceToParse[] generateRandomDocuments(int count) throws IOException {
        var docs = new SourceToParse[count];
        for (int i = 0; i < count; i++) {
            docs[i] = generateRandomDocument();
        }
        return docs;
    }

    private SourceToParse generateRandomDocument() throws IOException {
        var builder = XContentBuilder.builder(XContentType.JSON.xContent());

        builder.startObject();

        builder.startObject("kafka");
        {
            builder.startObject("log");
            {
                builder.field("component", randomString(10));
                builder.startArray("trace");
                {
                    builder.startObject();
                    {
                        builder.field("message", randomString(50));
                        builder.field("class", randomString(10));
                    }
                    builder.endObject();
                    builder.startObject();
                    {
                        builder.field("message", randomString(50));
                        builder.field("class", randomString(10));
                    }
                    builder.endObject();
                }
                builder.endArray();
                builder.field("thread", randomString(10));
                builder.field("class", randomString(10));

            }
            builder.endObject();
        }
        builder.endObject();

        builder.startObject("host");
        {
            builder.field("hostname", randomString(10));
            builder.startObject("os");
            {
                builder.field("name", randomString(10));
            }
            builder.endObject();

            builder.field("domain", randomString(10));
            builder.field("ip", randomIp());
            builder.field("name", randomString(10));
        }

        builder.endObject();

        builder.endObject();

        return new SourceToParse(UUIDs.randomBase64UUID(), BytesReference.bytes(builder), XContentType.JSON);
    }

    private String randomIp() {
        return "" + random.nextInt(255) + '.' + random.nextInt(255) + '.' + random.nextInt(255) + '.' + random.nextInt(255);
    }

    private String randomString(int maxLength) {
        var length = random.nextInt(maxLength);
        var builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((byte) (32 + random.nextInt(94)));
        }
        return builder.toString();
    }
}
