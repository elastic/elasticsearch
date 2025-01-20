/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.index.mapper.SourceFieldMapper;

import java.io.IOException;
import java.util.List;

public class DataStreamLicenceDowngradeIT extends DataStreamLicenseChangeIT {
    @Override
    protected void applyInitialLicense() throws IOException {
        startTrial();
    }

    @Override
    protected void licenseChange() throws IOException {
        startBasic();
    }

    @Override
    protected List<TestCase> cases() {
        return List.of(new TestCase() {
            @Override
            public String dataStreamName() {
                return "logs-test-regular";
            }

            @Override
            public String indexMode() {
                return "logsdb";
            }

            @Override
            public void prepareDataStream() throws IOException {
                assertOK(createDataStream(client(), dataStreamName()));
            }

            @Override
            public void rollover() throws IOException {
                rolloverDataStream(client(), dataStreamName());
            }

            @Override
            public SourceFieldMapper.Mode initialMode() {
                return SourceFieldMapper.Mode.SYNTHETIC;
            }

            @Override
            public SourceFieldMapper.Mode finalMode() {
                return SourceFieldMapper.Mode.STORED;
            }
        }, new TestCase() {
            private static final String sourceModeOverride = """
                {
                  "template": {
                    "settings": {
                      "index": {
                        "mapping.source.mode": "SYNTHETIC"
                      }
                    }
                  }
                }""";

            @Override
            public String dataStreamName() {
                return "logs-test-explicit-synthetic";
            }

            @Override
            public String indexMode() {
                return "logsdb";
            }

            @Override
            public void prepareDataStream() throws IOException {
                assertOK(putComponentTemplate(client(), "logs@custom", sourceModeOverride));
                assertOK(createDataStream(client(), dataStreamName()));
                assertOK(removeComponentTemplate(client(), "logs@custom"));
            }

            @Override
            public void rollover() throws IOException {
                assertOK(putComponentTemplate(client(), "logs@custom", sourceModeOverride));
                rolloverDataStream(client(), dataStreamName());
                assertOK(removeComponentTemplate(client(), "logs@custom"));
            }

            @Override
            public SourceFieldMapper.Mode initialMode() {
                return SourceFieldMapper.Mode.SYNTHETIC;
            }

            @Override
            public SourceFieldMapper.Mode finalMode() {
                return SourceFieldMapper.Mode.STORED;
            }
        }, new TestCase() {
            private static final String sourceModeOverride = """
                {
                  "template": {
                    "settings": {
                      "index": {
                        "mapping.source.mode": "STORED"
                      }
                    }
                  }
                }""";

            @Override
            public String dataStreamName() {
                return "logs-test-explicit-stored";
            }

            @Override
            public String indexMode() {
                return "logsdb";
            }

            @Override
            public void prepareDataStream() throws IOException {
                assertOK(putComponentTemplate(client(), "logs@custom", sourceModeOverride));
                assertOK(createDataStream(client(), dataStreamName()));
                assertOK(removeComponentTemplate(client(), "logs@custom"));
            }

            @Override
            public void rollover() throws IOException {
                assertOK(putComponentTemplate(client(), "logs@custom", sourceModeOverride));
                rolloverDataStream(client(), dataStreamName());
                assertOK(removeComponentTemplate(client(), "logs@custom"));
            }

            @Override
            public SourceFieldMapper.Mode initialMode() {
                return SourceFieldMapper.Mode.STORED;
            }

            @Override
            public SourceFieldMapper.Mode finalMode() {
                return SourceFieldMapper.Mode.STORED;
            }
        }, new TestCase() {
            @Override
            public String dataStreamName() {
                return "tsdb-test-regular";
            }

            @Override
            public String indexMode() {
                return "time_series";
            }

            @Override
            public void prepareDataStream() throws IOException {
                var componentTemplate = """
                    {
                      "template": {
                        "settings": {
                          "index": {
                            "mode": "time_series",
                            "routing_path": ["dim"]
                          }
                        },
                        "mappings": {
                          "properties": {
                            "dim": {
                              "type": "keyword",
                              "time_series_dimension": true
                            }
                          }
                        }
                      }
                    }
                    """;
                assertOK(putComponentTemplate(client(), "tsdb-test-regular-component", componentTemplate));

                var template = """
                    {
                      "index_patterns": ["tsdb-test-regular"],
                      "priority": 100,
                      "data_stream": {},
                      "composed_of": ["tsdb-test-regular-component"]
                    }
                    """;

                putTemplate(client(), "tsdb-test-regular-template", template);
                assertOK(createDataStream(client(), dataStreamName()));
            }

            @Override
            public void rollover() throws IOException {
                rolloverDataStream(client(), dataStreamName());
            }

            @Override
            public SourceFieldMapper.Mode initialMode() {
                return SourceFieldMapper.Mode.SYNTHETIC;
            }

            @Override
            public SourceFieldMapper.Mode finalMode() {
                return SourceFieldMapper.Mode.STORED;
            }
        }, new TestCase() {
            @Override
            public String dataStreamName() {
                return "tsdb-test-synthetic";
            }

            @Override
            public String indexMode() {
                return "time_series";
            }

            @Override
            public void prepareDataStream() throws IOException {
                var componentTemplate = """
                    {
                      "template": {
                        "settings": {
                          "index": {
                            "mode": "time_series",
                            "routing_path": ["dim"],
                            "mapping.source.mode": "SYNTHETIC"
                          }
                        },
                        "mappings": {
                          "properties": {
                            "dim": {
                              "type": "keyword",
                              "time_series_dimension": true
                            }
                          }
                        }
                      }
                    }
                    """;
                assertOK(putComponentTemplate(client(), "tsdb-test-synthetic-component", componentTemplate));

                var template = """
                    {
                      "index_patterns": ["tsdb-test-synthetic"],
                      "priority": 100,
                      "data_stream": {},
                      "composed_of": ["tsdb-test-synthetic-component"]
                    }
                    """;

                putTemplate(client(), "tsdb-test-synthetic-template", template);
                assertOK(createDataStream(client(), dataStreamName()));
            }

            @Override
            public void rollover() throws IOException {
                rolloverDataStream(client(), dataStreamName());
            }

            @Override
            public SourceFieldMapper.Mode initialMode() {
                return SourceFieldMapper.Mode.SYNTHETIC;
            }

            @Override
            public SourceFieldMapper.Mode finalMode() {
                return SourceFieldMapper.Mode.STORED;
            }
        }, new TestCase() {
            @Override
            public String dataStreamName() {
                return "tsdb-test-stored";
            }

            @Override
            public String indexMode() {
                return "time_series";
            }

            @Override
            public void prepareDataStream() throws IOException {
                var componentTemplate = """
                    {
                      "template": {
                        "settings": {
                          "index": {
                            "mode": "time_series",
                            "routing_path": ["dim"],
                            "mapping.source.mode": "STORED"
                          }
                        },
                        "mappings": {
                          "properties": {
                            "dim": {
                              "type": "keyword",
                              "time_series_dimension": true
                            }
                          }
                        }
                      }
                    }
                    """;
                assertOK(putComponentTemplate(client(), "tsdb-test-stored-component", componentTemplate));

                var template = """
                    {
                      "index_patterns": ["tsdb-test-stored"],
                      "priority": 100,
                      "data_stream": {},
                      "composed_of": ["tsdb-test-stored-component"]
                    }
                    """;

                putTemplate(client(), "tsdb-test-stored-template", template);
                assertOK(createDataStream(client(), dataStreamName()));
            }

            @Override
            public void rollover() throws IOException {
                rolloverDataStream(client(), dataStreamName());
            }

            @Override
            public SourceFieldMapper.Mode initialMode() {
                return SourceFieldMapper.Mode.STORED;
            }

            @Override
            public SourceFieldMapper.Mode finalMode() {
                return SourceFieldMapper.Mode.STORED;
            }
        },

            new TestCase() {
                @Override
                public String dataStreamName() {
                    return "standard";
                }

                @Override
                public String indexMode() {
                    return "standard";
                }

                @Override
                public void prepareDataStream() throws IOException {
                    var template = """
                        {
                          "index_patterns": ["standard"],
                          "priority": 100,
                          "data_stream": {},
                          "composed_of": []
                        }
                        """;

                    putTemplate(client(), "standard-template", template);
                    assertOK(createDataStream(client(), dataStreamName()));
                }

                @Override
                public void rollover() throws IOException {
                    rolloverDataStream(client(), dataStreamName());
                }

                @Override
                public SourceFieldMapper.Mode initialMode() {
                    return SourceFieldMapper.Mode.STORED;
                }

                @Override
                public SourceFieldMapper.Mode finalMode() {
                    return SourceFieldMapper.Mode.STORED;
                }
            },
            new TestCase() {
                @Override
                public String dataStreamName() {
                    return "standard-synthetic";
                }

                @Override
                public String indexMode() {
                    return "standard";
                }

                @Override
                public void prepareDataStream() throws IOException {
                    var componentTemplate = """
                        {
                          "template": {
                            "settings": {
                              "index": {
                                "mapping.source.mode": "SYNTHETIC"
                              }
                            }
                          }
                        }
                        """;
                    assertOK(putComponentTemplate(client(), "standard-synthetic-component", componentTemplate));

                    var template = """
                        {
                          "index_patterns": ["standard-synthetic"],
                          "priority": 100,
                          "data_stream": {},
                          "composed_of": ["standard-synthetic-component"]
                        }
                        """;

                    putTemplate(client(), "standard-synthetic-template", template);
                    assertOK(createDataStream(client(), dataStreamName()));
                }

                @Override
                public void rollover() throws IOException {
                    rolloverDataStream(client(), dataStreamName());
                }

                @Override
                public SourceFieldMapper.Mode initialMode() {
                    return SourceFieldMapper.Mode.SYNTHETIC;
                }

                @Override
                public SourceFieldMapper.Mode finalMode() {
                    return SourceFieldMapper.Mode.STORED;
                }
            },
            new TestCase() {
                @Override
                public String dataStreamName() {
                    return "standard-stored";
                }

                @Override
                public String indexMode() {
                    return "standard";
                }

                @Override
                public void prepareDataStream() throws IOException {
                    var componentTemplate = """
                        {
                          "template": {
                            "settings": {
                              "index": {
                                "mapping.source.mode": "STORED"
                              }
                            }
                          }
                        }
                        """;
                    assertOK(putComponentTemplate(client(), "standard-stored-component", componentTemplate));

                    var template = """
                        {
                          "index_patterns": ["standard-stored"],
                          "priority": 100,
                          "data_stream": {},
                          "composed_of": ["standard-stored-component"]
                        }
                        """;

                    putTemplate(client(), "standard-stored-template", template);
                    assertOK(createDataStream(client(), dataStreamName()));
                }

                @Override
                public void rollover() throws IOException {
                    rolloverDataStream(client(), dataStreamName());
                }

                @Override
                public SourceFieldMapper.Mode initialMode() {
                    return SourceFieldMapper.Mode.STORED;
                }

                @Override
                public SourceFieldMapper.Mode finalMode() {
                    return SourceFieldMapper.Mode.STORED;
                }
            }
        );
    }
}
