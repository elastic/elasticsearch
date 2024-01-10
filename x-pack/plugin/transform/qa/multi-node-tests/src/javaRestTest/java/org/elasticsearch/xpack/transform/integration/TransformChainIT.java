/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformChainIT extends TransformRestTestCase {

    private static final String DEST_INDEX_TEMPLATE = """
        {
          "index_patterns": [ "my-transform-*-dest" ],
          "mappings": {
            "properties": {
              "timestamp": {
                "type": "date"
              },
              "user_id": {
                "type": "keyword"
              },
              "stars": {
                "type": "integer"
              }
            }
          }
        }""";

    private static final String TRANSFORM_CONFIG_TEMPLATE = """
        {
          "source": {
            "index": "%s"
          },
          "dest": {
            "index": "%s"
          },
          "sync": {
            "time": {
              "field": "timestamp"
            }
          },
          "frequency": "%s",
          "pivot": {
            "group_by": {
              "timestamp": {
                "date_histogram": {
                  "field": "timestamp",
                  "fixed_interval": "%s"
                }
              },
              "user_id": {
                "terms": {
                  "field": "user_id"
                }
              }
            },
            "aggregations": {
              "stars": {
                "sum": {
                  "field": "stars"
                }
              }
            }
          },
          "settings": {
            "unattended": true,
            "deduce_mappings": %s
          }
        }""";

    private TestThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public void testChainedTransforms() throws Exception {
        String reviewsIndexName = "reviews";
        final int numDocs = 100;
        createReviewsIndex(reviewsIndexName, numDocs, 100, TransformIT::getUserIdForRow, TransformIT::getDateStringForRow);

        // Create destination index template. It will be used by all the transforms in this test.
        Request createIndexTemplateRequest = new Request("PUT", "_template/test_dest_index_template");
        createIndexTemplateRequest.setJsonEntity(DEST_INDEX_TEMPLATE);
        createIndexTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        assertAcknowledged(client().performRequest(createIndexTemplateRequest));

        final int numberOfTransforms = 3;
        List<String> transformIds = new ArrayList<>(numberOfTransforms);
        // Create the chain of transforms. Previous transform's destination index becomes next transform's source index.
        for (int i = 0; i < numberOfTransforms; ++i) {
            String transformId = "my-transform-" + i;
            transformIds.add(transformId);
            // Set up the transform so that its source index is the destination index of the previous transform in the chain.
            // The number of documents is expected to be the same in all the indices.
            String sourceIndex = i == 0 ? reviewsIndexName : "my-transform-" + (i - 1) + "-dest";
            String destIndex = transformId + "-dest";
            assertFalse(indexExists(destIndex));

            assertAcknowledged(putTransform(transformId, createTransformConfig(sourceIndex, destIndex), true, RequestOptions.DEFAULT));
        }

        List<String> transformIdsShuffled = new ArrayList<>(transformIds);
        Collections.shuffle(transformIdsShuffled, random());
        // Start all the transforms in random order so that sometimes the transform later in the chain needs to wait for its source index
        // to become available.
        for (String transformId : transformIdsShuffled) {
            startTransform(transformId, RequestOptions.DEFAULT);
        }

        // Wait for the transforms to finish processing. Since the transforms are continuous, we cannot wait for them to be STOPPED.
        // Instead, we wait for the expected number of processed documents.
        assertBusy(() -> {
            for (String transformId : transformIds) {
                Map<?, ?> stats = getTransformStats(transformId);
                // Verify that all the documents got processed.
                assertThat(
                    "Stats were: " + stats,
                    XContentMapValues.extractValue(stats, "stats", "documents_processed"),
                    is(equalTo(numDocs))
                );
            }
        }, 60, TimeUnit.SECONDS);

        // Stop all the transforms.
        for (String transformId : transformIds) {
            stopTransform(transformId);
        }
        // Delete all the transforms.
        for (String transformId : transformIds) {
            deleteTransform(transformId);
        }
    }

    private static String createTransformConfig(String sourceIndex, String destIndex) {
        return Strings.format(TRANSFORM_CONFIG_TEMPLATE, sourceIndex, destIndex, "1s", "1s", randomBoolean());
    }
}
