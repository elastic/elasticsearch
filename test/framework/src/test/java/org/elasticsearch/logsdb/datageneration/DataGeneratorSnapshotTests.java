/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logsdb.datageneration.arbitrary.Arbitrary;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

public class DataGeneratorSnapshotTests extends ESTestCase {
    public void testSnapshot() throws Exception {
        var dataGenerator = new DataGenerator(new DataGeneratorSpecification(5, 2, new TestArbitrary()));

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
                        "type" : "keyword"
                      },
                      "f3" : {
                        "properties" : {
                          "f4" : {
                            "type" : "long"
                          },
                          "f5" : {
                            "type" : "keyword"
                          }
                        }
                      }
                    }
                  },
                  "f6" : {
                    "type" : "long"
                  }
                }
              }
            }""";

        var expectedDocument = """
            {
              "f1" : {
                "f2" : "string1",
                "f3" : {
                  "f4" : 0,
                  "f5" : "string2"
                }
              },
              "f6" : 1
            }""";

        assertEquals(expectedMapping, Strings.toString(mapping));
        assertEquals(expectedDocument, Strings.toString(document));
    }

    private class TestArbitrary implements Arbitrary {
        private boolean generateSubObject = true;
        private int generatedFields = 0;
        private FieldType fieldType = FieldType.KEYWORD;
        private long longValue = 0;
        private long generatedStringValues = 0;

        @Override
        public boolean generateSubObject() {
            if (generateSubObject) {
                generateSubObject = false;
                return true;
            }

            generateSubObject = true;
            return false;
        }

        @Override
        public int childFieldCount(int lowerBound, int upperBound) {
            assert lowerBound < 2 && upperBound > 2;
            return 2;
        }

        @Override
        public String fieldName(int lengthLowerBound, int lengthUpperBound) {
            return "f" + (generatedFields++ + 1);
        }

        @Override
        public FieldType fieldType() {
            if (fieldType == FieldType.KEYWORD) {
                fieldType = FieldType.LONG;
                return FieldType.KEYWORD;
            }

            fieldType = FieldType.KEYWORD;
            return FieldType.LONG;
        }

        @Override
        public long longValue() {
            return longValue++;
        }

        @Override
        public String stringValue(int lengthLowerBound, int lengthUpperBound) {
            return "string" + (generatedStringValues++ + 1);
        }
    };
}
