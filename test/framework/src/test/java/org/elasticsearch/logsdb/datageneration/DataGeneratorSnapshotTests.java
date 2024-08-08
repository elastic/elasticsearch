/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Optional;

public class DataGeneratorSnapshotTests extends ESTestCase {
    public void testSnapshot() throws Exception {
        var dataGenerator = new DataGenerator(
            DataGeneratorSpecification.builder()
                .withDataSourceHandlers(List.of(new DataSourceOverrides()))
                .withMaxFieldCountPerLevel(5)
                .withMaxObjectDepth(2)
                .build()
        );

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        dataGenerator.writeMapping(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        dataGenerator.generateDocument(document);

        var expectedMapping = """
            {
              "_doc" : {
                "properties" : {
                  "f1" : {
                    "properties" : {
                      "f2" : {
                        "properties" : {
                          "f3" : {
                            "type" : "keyword"
                          },
                          "f4" : {
                            "type" : "long"
                          }
                        }
                      },
                      "f5" : {
                        "properties" : {
                          "f6" : {
                            "type" : "keyword"
                          },
                          "f7" : {
                            "type" : "long"
                          }
                        }
                      }
                    }
                  },
                  "f8" : {
                    "type" : "nested",
                    "properties" : {
                      "f9" : {
                        "type" : "nested",
                        "properties" : {
                          "f10" : {
                            "type" : "keyword"
                          },
                          "f11" : {
                            "type" : "long"
                          }
                        }
                      },
                      "f12" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""";

        var expectedDocument = """
            {
              "f1" : [
                {
                  "f2" : {
                    "f3" : [
                      null,
                      "string1"
                    ],
                    "f4" : 0
                  },
                  "f5" : {
                    "f6" : "string2",
                    "f7" : null
                  }
                },
                {
                  "f2" : {
                    "f3" : [
                      "string3",
                      "string4"
                    ],
                    "f4" : 1
                  },
                  "f5" : {
                    "f6" : null,
                    "f7" : 2
                  }
                }
              ],
              "f8" : {
                "f9" : {
                  "f10" : [
                    "string5",
                    "string6"
                  ],
                  "f11" : null
                },
                "f12" : "string7"
              }
            }""";

        assertEquals(expectedMapping, Strings.toString(mapping));
        assertEquals(expectedDocument, Strings.toString(document));
    }

    private static class DataSourceOverrides implements DataSourceHandler {
        private long longValue = 0;
        private long generatedStrings = 0;
        private int generateNullChecks = 0;
        private int generateArrayChecks = 0;
        private boolean producedObjectArray = false;
        private FieldType fieldType = FieldType.KEYWORD;
        private final StaticChildFieldGenerator childFieldGenerator = new StaticChildFieldGenerator();

        @Override
        public DataSourceResponse.LongGenerator handle(DataSourceRequest.LongGenerator request) {
            return new DataSourceResponse.LongGenerator(() -> longValue++);
        }

        @Override
        public DataSourceResponse.StringGenerator handle(DataSourceRequest.StringGenerator request) {
            return new DataSourceResponse.StringGenerator(() -> "string" + (generatedStrings++ + 1));
        }

        @Override
        public DataSourceResponse.NullWrapper handle(DataSourceRequest.NullWrapper request) {
            return new DataSourceResponse.NullWrapper((values) -> () -> generateNullChecks++ % 4 == 0 ? null : values.get());
        }

        @Override
        public DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper request) {

            return new DataSourceResponse.ArrayWrapper((values) -> () -> {
                if (generateArrayChecks++ % 4 == 0) {
                    // we have nulls so can't use List.of
                    return new Object[] { values.get(), values.get() };
                }

                return values.get();
            });
        }

        @Override
        public DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {

            return childFieldGenerator;
        }

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
            return new DataSourceResponse.ObjectArrayGenerator(() -> {
                if (producedObjectArray == false) {
                    producedObjectArray = true;
                    return Optional.of(2);
                }

                return Optional.empty();
            });
        }

        @Override
        public DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
            return new DataSourceResponse.FieldTypeGenerator(() -> {
                if (fieldType == FieldType.KEYWORD) {
                    fieldType = FieldType.LONG;
                    return FieldType.KEYWORD;
                }

                fieldType = FieldType.KEYWORD;
                return FieldType.LONG;
            });
        }
    }

    private static class StaticChildFieldGenerator implements DataSourceResponse.ChildFieldGenerator {
        private int generatedFields = 0;

        @Override
        public int generateChildFieldCount() {
            return 2;
        }

        @Override
        public boolean generateNestedSubObject() {
            return generatedFields > 6 && generatedFields < 12;
        }

        @Override
        public boolean generateRegularSubObject() {
            return generatedFields < 6;
        }

        @Override
        public String generateFieldName() {
            return "f" + (generatedFields++ + 1);
        }
    }
}
