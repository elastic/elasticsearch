/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RestSimulateIngestActionTests extends ESTestCase {

    public void testConvertToBulkRequestXContentBytes() throws Exception {
        {
            // No index, no id, which we expect to be fine:
            String simulateRequestJson = """
                {
                  "docs": [
                    {
                      "_source": {
                        "my-keyword-field": "FOO"
                      }
                    },
                    {
                      "_source": {
                        "my-keyword-field": "BAR"
                      }
                    }
                  ],
                  "pipeline_substitutions": {
                    "my-pipeline-2": {
                      "processors": [
                        {
                          "set": {
                            "field": "my-new-boolean-field",
                            "value": true
                          }
                        }
                      ]
                    }
                  }
                }
                """;
            String bulkRequestJson = """
                {"index":{}}
                {"my-keyword-field":"FOO"}
                {"index":{}}
                {"my-keyword-field":"BAR"}
                                """;
            testInputJsonConvertsToOutputJson(simulateRequestJson, bulkRequestJson);
        }

        {
            // index and id:
            String simulateRequestJson = """
                {
                  "docs": [
                    {
                      "_index": "index",
                      "_id": "123",
                      "_source": {
                        "foo": "bar"
                      }
                    },
                    {
                      "_index": "index",
                      "_id": "456",
                      "_source": {
                        "foo": "rab"
                      }
                    }
                  ]
                }
                """;
            String bulkRequestJson = """
                {"index":{"_index":"index","_id":"123"}}
                {"foo":"bar"}
                {"index":{"_index":"index","_id":"456"}}
                {"foo":"rab"}
                                                """;
            testInputJsonConvertsToOutputJson(simulateRequestJson, bulkRequestJson);
        }

        {
            // We expect an IllegalArgumentException if there are no docs:
            String simulateRequestJson = """
                {
                  "docs": [
                  ]
                }
                """;
            String bulkRequestJson = """
                {"index":{"_index":"index","_id":"123"}}
                {"foo":"bar"}
                {"index":{"_index":"index","_id":"456"}}
                {"foo":"rab"}
                                                """;
            expectThrows(IllegalArgumentException.class, () -> testInputJsonConvertsToOutputJson(simulateRequestJson, bulkRequestJson));
        }

        {
            // non-trivial source:
            String simulateRequestJson = """
                {
                  "docs": [
                    {
                      "_index": "index",
                      "_id": "123",
                      "_source": {
                        "foo": "bar",
                        "some_object": {
                          "prop1": "val1",
                          "some_array": [1, 2, 3, 4]
                        }
                      }
                    }
                  ]
                }
                """;
            String bulkRequestJson = """
                {"index":{"_index":"index","_id":"123"}}
                {"some_object":{"prop1":"val1","some_array":[1,2,3,4]},"foo":"bar"}
                                                """;
            testInputJsonConvertsToOutputJson(simulateRequestJson, bulkRequestJson);
        }
    }

    private void testInputJsonConvertsToOutputJson(String inputJson, String expectedOutputJson) throws Exception {
        Map<String, Object> sourceMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), inputJson, false);
        BytesReference bulkXcontentBytes = RestSimulateIngestAction.convertToBulkRequestXContentBytes(sourceMap);
        String bulkRequestJson = XContentHelper.convertToJson(bulkXcontentBytes, false, XContentType.JSON);
        assertThat(bulkRequestJson, equalTo(expectedOutputJson));
    }
}
