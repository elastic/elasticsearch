/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.common.Strings;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.TemplateGenerator;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataGenerationSnapshotTests extends ESTestCase {
    public void testSnapshot() throws Exception {
        var specification = DataGeneratorSpecification.builder()
            .withDataSourceHandlers(List.of(new DataSourceOverrides()))
            .withMaxFieldCountPerLevel(5)
            .withMaxObjectDepth(2)
            .build();

        var template = new TemplateGenerator(specification).generate();
        var mapping = new MappingGenerator(specification).generate(template);

        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        mappingXContent.map(mapping.raw());

        var documentXContent = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        documentXContent.map(new DocumentGenerator(specification).generate(template, mapping));

        var expectedMapping = """
            {
              "_doc" : {
                "dynamic" : "false",
                "properties" : {
                  "f1" : {
                    "dynamic" : "false",
                    "properties" : {
                      "f2" : {
                        "dynamic" : "false",
                        "properties" : {
                          "f3" : {
                            "store" : "true",
                            "type" : "keyword"
                          },
                          "f4" : {
                            "index" : "false",
                            "type" : "long"
                          }
                        },
                        "type" : "object"
                      },
                      "f5" : {
                        "dynamic" : "false",
                        "properties" : {
                          "f6" : {
                            "store" : "true",
                            "type" : "keyword"
                          },
                          "f7" : {
                            "dynamic" : "false",
                            "properties" : {
                              "f8" : {
                                "index" : "false",
                                "type" : "long"
                              },
                              "f9" : {
                                "store" : "true",
                                "type" : "keyword"
                              }
                            },
                            "type" : "nested"
                          }
                        },
                        "type" : "object"
                      }
                    },
                    "type" : "object"
                  },
                  "f10" : {
                    "dynamic" : "false",
                    "properties" : {
                      "f11" : {
                        "dynamic" : "false",
                        "properties" : {
                          "f12" : {
                            "index" : "false",
                            "type" : "long"
                          },
                          "f13" : {
                            "store" : "true",
                            "type" : "keyword"
                          }
                        },
                        "type" : "nested"
                      },
                      "f14" : {
                        "index" : "false",
                        "type" : "long"
                      }
                    },
                    "type" : "nested"
                  }
                }
              }
            }""";

        var expectedDocument = """
            {
              "f1" : {
                "f2" : {
                  "f3" : null,
                  "f4" : 3
                },
                "f5" : {
                  "f6" : [
                    "string4",
                    "string5"
                  ],
                  "f7" : {
                    "f8" : null,
                    "f9" : "string6"
                  }
                }
              },
              "f10" : [
                {
                  "f11" : {
                    "f12" : [
                      null,
                      0
                    ],
                    "f13" : "string1"
                  },
                  "f14" : 1
                },
                {
                  "f11" : {
                    "f12" : null,
                    "f13" : [
                      "string2",
                      "string3"
                    ]
                  },
                  "f14" : 2
                }
              ]
            }""";

        assertEquals(expectedMapping, Strings.toString(mappingXContent));
        assertEquals(expectedDocument, Strings.toString(documentXContent));
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
                    return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(FieldType.KEYWORD.toString());
                }

                fieldType = FieldType.KEYWORD;
                return new DataSourceResponse.FieldTypeGenerator.FieldTypeInfo(FieldType.LONG.toString());
            });
        }

        @Override
        public DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
            return new DataSourceResponse.DynamicMappingGenerator((ignored) -> false);
        }

        @Override
        public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
            if (request.fieldType().equals(FieldType.KEYWORD.toString())) {
                return new DataSourceResponse.LeafMappingParametersGenerator(() -> Map.of("store", "true"));
            }

            if (request.fieldType().equals(FieldType.LONG.toString())) {
                return new DataSourceResponse.LeafMappingParametersGenerator(() -> Map.of("index", "false"));
            }

            return null;
        }

        @Override
        public DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
            return new DataSourceResponse.ObjectMappingParametersGenerator(() -> new HashMap<>(Map.of("dynamic", "false")));
        }
    }

    private static class StaticChildFieldGenerator implements DataSourceResponse.ChildFieldGenerator {
        private int generatedFields = 0;

        @Override
        public int generateChildFieldCount() {
            return 2;
        }

        @Override
        public boolean generateDynamicSubObject() {
            return false;
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
