/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.Seed;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

@SuppressForbidden(
    reason = "There is a lot of randomness in the code we are testing here. "
        + "We want one static snapshot test so that we can write strong asserts."
)
@Seed("CBA335BE079C1BB4")
public class DataGeneratorSnapshotTests extends ESTestCase {
    public void testSnapshot() throws IOException {
        var dataGenerator = new DataGenerator(new DataGeneratorSpecification(5, 3));

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        dataGenerator.writeMapping(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        dataGenerator.generateDocument(document);

        var expectedMapping = """
            {
              "_doc" : {
                "properties" : {
                  "VDJcDWqhRMInRaGEC" : {
                    "properties" : {
                      "VceyNMHkuypgsatswkntOwbolEytbuzxIlwbMmVzc" : {
                        "type" : "long"
                      },
                      "suHdsWHVabAJlexlBDYHtrNhzAw" : {
                        "type" : "long"
                      }
                    }
                  },
                  "KusUTflTYfXPZ" : {
                    "type" : "long"
                  },
                  "wupnzcaMBLYWNiNWrppFCZMKzngSfCaiSmfzOnUhCsN" : {
                    "properties" : {
                      "BPwGQoGLfLqoceHpMN" : {
                        "properties" : {
                          "imAizcWraQBGGBGC" : {
                            "type" : "long"
                          }
                        }
                      },
                      "USYnVcJOzULGRkSaeEXxegWQfYxSKDlHyzdcHAVJYrSkqL" : {
                        "type" : "keyword"
                      },
                      "nPOWflnHpagenXzbkuDiVoDzKrPrwiFCqVNFY" : {
                        "type" : "long"
                      },
                      "ANjouXkcCgiKAYFSAaFifLsRdVErSzEJwknUFNh" : {
                        "type" : "keyword"
                      },
                      "KqYfTcchwWsuPS" : {
                        "type" : "long"
                      }
                    }
                  },
                  "hGncaCPHdprQLTUm" : {
                    "properties" : {
                      "BYjMnZCHbAFJuuGILjwTzzArOXUcizvvsfSxuKdS" : {
                        "type" : "keyword"
                      },
                      "LNUeEThrcwKuXOindX" : {
                        "type" : "long"
                      },
                      "LGabYPznMGl" : {
                        "type" : "keyword"
                      }
                    }
                  }
                }
              }
            }""";

        var expectedDocument = """
            {
              "VDJcDWqhRMInRaGEC" : {
                "VceyNMHkuypgsatswkntOwbolEytbuzxIlwbMmVzc" : 6599619436245425830,
                "suHdsWHVabAJlexlBDYHtrNhzAw" : 5400071568447452818
              },
              "KusUTflTYfXPZ" : -841868371946563243,
              "wupnzcaMBLYWNiNWrppFCZMKzngSfCaiSmfzOnUhCsN" : {
                "BPwGQoGLfLqoceHpMN" : {
                  "imAizcWraQBGGBGC" : 6120144518695593976
                },
                "USYnVcJOzULGRkSaeEXxegWQfYxSKDlHyzdcHAVJYrSkqL" : "QfiG",
                "nPOWflnHpagenXzbkuDiVoDzKrPrwiFCqVNFY" : -7572306868127563201,
                "ANjouXkcCgiKAYFSAaFifLsRdVErSzEJwknUFNh" : "sGbtJzQDcLcNRXBVhevoopcrjbUYvWdOWUFjQCtuVRDfQp",
                "KqYfTcchwWsuPS" : 7897327850725852962
              },
              "hGncaCPHdprQLTUm" : {
                "BYjMnZCHbAFJuuGILjwTzzArOXUcizvvsfSxuKdS" : "EguFyVgbRntxlKObhPaBpMLOLa",
                "LNUeEThrcwKuXOindX" : -8907455453724983858,
                "LGabYPznMGl" : "NczMmXRmGOueSrudXPJwSmUZQrUKOjMuXH"
              }
            }""";

        assertEquals(expectedMapping, Strings.toString(mapping));
        assertEquals(expectedDocument, Strings.toString(document));
    }
}
