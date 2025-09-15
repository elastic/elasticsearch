/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.ingest;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
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

    public void testSimulateIngestRestToXContentListener() throws Exception {
        // First, make sure it works with success responses:
        BulkItemResponse[] responses = new BulkItemResponse[3];
        responses[0] = getSuccessBulkItemResponse("123", "{\"foo\": \"bar\"}");
        responses[1] = getFailureBulkItemResponse("678", "This has failed");
        responses[2] = getSuccessBulkItemResponse("456", "{\"bar\": \"baz\"}");
        BulkResponse bulkResponse = new BulkResponse(responses, randomLongBetween(0, 50000));
        String expectedXContent = """
            {
              "docs" : [
                {
                  "doc" : {
                    "_id" : "123",
                    "_index" : "index1",
                    "_version" : 3,
                    "_source" : {
                      "foo" : "bar"
                    },
                    "executed_pipelines" : [
                      "pipeline1",
                      "pipeline2"
                    ],
                    "ignored_fields" : [
                      {
                        "field" : "abc"
                      },
                      {
                        "field" : "def"
                      }
                    ]
                  }
                },
                {
                  "doc" : {
                    "_id" : "678",
                    "_index" : "index1",
                    "error" : {
                      "type" : "runtime_exception",
                      "reason" : "This has failed"
                    }
                  }
                },
                {
                  "doc" : {
                    "_id" : "456",
                    "_index" : "index1",
                    "_version" : 3,
                    "_source" : {
                      "bar" : "baz"
                    },
                    "executed_pipelines" : [
                      "pipeline1",
                      "pipeline2"
                    ],
                    "ignored_fields" : [
                      {
                        "field" : "abc"
                      },
                      {
                        "field" : "def"
                      }
                    ]
                  }
                }
              ]
            }""";
        testSimulateIngestRestToXContentListener(bulkResponse, expectedXContent);
    }

    private BulkItemResponse getFailureBulkItemResponse(String id, String failureMessage) {
        return BulkItemResponse.failure(
            randomInt(),
            randomFrom(DocWriteRequest.OpType.values()),
            new BulkItemResponse.Failure("index1", id, new RuntimeException(failureMessage))
        );
    }

    private BulkItemResponse getSuccessBulkItemResponse(String id, String source) {
        ByteBuffer[] sourceByteBuffer = new ByteBuffer[1];
        sourceByteBuffer[0] = ByteBuffer.wrap(source.getBytes(StandardCharsets.UTF_8));
        return BulkItemResponse.success(
            randomInt(),
            randomFrom(DocWriteRequest.OpType.values()),
            new SimulateIndexResponse(
                id,
                "index1",
                3,
                BytesReference.fromByteBuffers(sourceByteBuffer),
                XContentType.JSON,
                List.of("pipeline1", "pipeline2"),
                List.of("abc", "def"),
                null
            )
        );
    }

    private void testSimulateIngestRestToXContentListener(BulkResponse bulkResponse, String expectedResult) throws Exception {
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build();
        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        RestSimulateIngestAction.SimulateIngestRestToXContentListener listener =
            new RestSimulateIngestAction.SimulateIngestRestToXContentListener(new AbstractRestChannel(request, true) {
                @Override
                public void sendResponse(RestResponse response) {
                    responseSetOnce.set(response);
                }
            });
        listener.onResponse(bulkResponse);
        RestResponse response = responseSetOnce.get();
        String bulkRequestJson = XContentHelper.convertToJson(response.content(), true, true, XContentType.JSON);
        assertThat(bulkRequestJson, equalTo(expectedResult));
    }
}
