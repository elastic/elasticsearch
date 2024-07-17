/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

public class DataGeneratorSnapshotTests extends ESTestCase {
    public void testSnapshot() throws Exception {
        // This is a workaround to have a test with static random seed.
        // There is a lot of randomness in the code we are testing here.
        // We want one static snapshot test so that we can write strong asserts
        // and see the result that code produces.
        RandomizedContext.current().runWithPrivateRandomness(-2692230890836950060L, () -> {
            var dataGenerator = new DataGenerator(new DataGeneratorSpecification(5, 3));

            var mapping = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
            dataGenerator.writeMapping(mapping);

            var document = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
            dataGenerator.generateDocument(document);

            var expectedMapping = """
                {
                  "_doc" : {
                    "properties" : {
                      "WUUOoFnpLDUIryHzkHuVAqqnTkBopIdzZuloVPpbjL" : {
                        "type" : "keyword"
                      },
                      "XHCheCPhJCPhGaqXSrkURPhlfEMDDfftkFnWb" : {
                        "type" : "keyword"
                      },
                      "QHcrBsDwzJzAnBXVbWdn" : {
                        "type" : "long"
                      },
                      "jdBjxWEfbUOwfymlroNoTyFuRDzpUjnBL" : {
                        "properties" : {
                          "cVglqwvNgCF" : {
                            "type" : "keyword"
                          },
                          "ukXrhVCBqnmxxhodYCyCEcRfgHpJgfz" : {
                            "type" : "long"
                          },
                          "dkUCNNHoIDGtEFBzwgSQruICTjSWBsLEMoNR" : {
                            "type" : "long"
                          },
                          "rdNKpBHzdvwMcWxMNAUgfyirPXzNNyIaKkV" : {
                            "properties" : {
                              "cvlZWMzfBefQqdVeXtxJcONamiDIMRciTkctr" : {
                                "type" : "long"
                              }
                            }
                          }
                        }
                      },
                      "KLlJwqHjLYSu" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }""";

            var expectedDocument = """
                {
                  "WUUOoFnpLDUIryHzkHuVAqqnTkBopIdzZuloVPpbjL" : "kwaGeKdalYbOE",
                  "XHCheCPhJCPhGaqXSrkURPhlfEMDDfftkFnWb" : "iASNe",
                  "QHcrBsDwzJzAnBXVbWdn" : -2703143610541897792,
                  "jdBjxWEfbUOwfymlroNoTyFuRDzpUjnBL" : {
                    "cVglqwvNgCF" : "LWE",
                    "ukXrhVCBqnmxxhodYCyCEcRfgHpJgfz" : 7100672347108481398,
                    "dkUCNNHoIDGtEFBzwgSQruICTjSWBsLEMoNR" : -2819217416523838608,
                    "rdNKpBHzdvwMcWxMNAUgfyirPXzNNyIaKkV" : {
                      "cvlZWMzfBefQqdVeXtxJcONamiDIMRciTkctr" : -7590118516953240854
                    }
                  },
                  "KLlJwqHjLYSu" : "imc"
                }""";

            assertEquals(expectedMapping, Strings.toString(mapping));
            assertEquals(expectedDocument, Strings.toString(document));

            return null;
        });
    }
}
