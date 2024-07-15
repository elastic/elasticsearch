/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.datageneration.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.Seed;

import org.elasticsearch.common.Strings;
import org.elasticsearch.datastreams.logsdb.datageneration.DataGenerator;
import org.elasticsearch.datastreams.logsdb.datageneration.DataGeneratorSpecification;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

@Seed("895AC2F45AD0307B")
public class DataGeneratorSnapshotTest extends ESTestCase {
    public void testSnapshot() throws IOException {
        System.out.println(RandomizedContext.current().getRunnerSeedAsString());

        var dataGenerator = new DataGenerator(new DataGeneratorSpecification(5));

        var mapping = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        dataGenerator.writeMapping(mapping);

        var document = XContentBuilder.builder(XContentType.JSON.xContent()).prettyPrint();
        dataGenerator.generateDocument(document);

        var expectedMapping = """
            {
              "_doc" : {
                "properties" : {
                  "svFqiacudYguDdcVgcUpebl" : {
                    "properties" : {
                      "CatqyILgYGuwUVIEnfqDlpevMflGCMqUajRxqgjyz" : {
                        "type" : "keyword"
                      },
                      "KZdZrzmuekeLQNqvxUFmsmSoWfErw" : {
                        "type" : "long"
                      },
                      "kcVkFBpEaZSmxf" : {
                        "type" : "keyword"
                      },
                      "SctcN" : {
                        "type" : "long"
                      }
                    }
                  },
                  "tEgIvpoczqUQxhd" : {
                    "type" : "long"
                  },
                  "NzdTGhSwnfmSNmnEevaXafFZtDGwSu" : {
                    "type" : "keyword"
                  }
                }
              }
            }""";

        var expectedDocument = """
            {
              "svFqiacudYguDdcVgcUpebl" : {
                "CatqyILgYGuwUVIEnfqDlpevMflGCMqUajRxqgjyz" : "YfAhZTQCwciz",
                "KZdZrzmuekeLQNqvxUFmsmSoWfErw" : -3982310058869716458,
                "kcVkFBpEaZSmxf" : "My",
                "SctcN" : 8919247631554273185
              },
              "tEgIvpoczqUQxhd" : -4162190923370660906,
              "NzdTGhSwnfmSNmnEevaXafFZtDGwSu" : "FN"
            }""";

        assertEquals(expectedMapping, Strings.toString(mapping));
        assertEquals(expectedDocument, Strings.toString(document));
    }
}
