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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformChainIT extends TransformRestTestCase {

    private static final String SET_INGEST_TIME_PIPELINE = "set_ingest_time";
    private static final String TRANSFORM_CONFIG_TEMPLATE = """
        {
          "source": {
            "index": "%s"
          },
          "dest": {
            "index": "%s",
            "pipeline": "%s",
            "aliases": [
              {
                "alias": "%s"
              },
              {
                "alias": "%s",
                "move_on_creation": true
              }
            ]
          },
          "sync": {
            "time": {
              "field": "event.ingested",
              "delay": "10s"
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
            "deduce_mappings": %s,
            "use_point_in_time": %s
          }
        }""";

    private TestThreadPool threadPool;

    @Before
    public void setupTransformTests() throws IOException {
        threadPool = new TestThreadPool(getTestName());

        // Create destination index template. It will be used by all the transforms in this test.
        Request createIndexTemplateRequest = new Request("PUT", "_template/test_dest_index_template");
        createIndexTemplateRequest.setJsonEntity("""
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
            }""");
        createIndexTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        assertAcknowledged(client().performRequest(createIndexTemplateRequest));

        // Create ingest pipeline which sets event.ingested field. This is needed for transform's synchronisation to work correctly.
        Request putIngestPipelineRequest = new Request("PUT", "_ingest/pipeline/" + SET_INGEST_TIME_PIPELINE);
        putIngestPipelineRequest.setJsonEntity("""
            {
              "description": "Set ingest timestamp.",
              "processors": [
                {
                  "set": {
                    "field": "event.ingested",
                    "value": "{{{_ingest.timestamp}}}"
                  }
                }
              ]
            }""");
        assertOK(client().performRequest(putIngestPipelineRequest));

        // Set logging levels for debugging.
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity("""
            {
              "persistent": {
                "logger.org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer": "debug",
                "logger.org.elasticsearch.xpack.transform": "debug",
                "logger.org.elasticsearch.xpack.transform.notifications": "debug",
                "logger.org.elasticsearch.xpack.transform.transforms": "debug"
              }
            }""");
        assertOK(client().performRequest(settingsRequest));
    }

    @After
    public void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdown();
        }
    }

    public void testTwoChainedTransforms() throws Exception {
        testChainedTransforms(2);
    }

    public void testThreeChainedTransforms() throws Exception {
        testChainedTransforms(3);
    }

    private void testChainedTransforms(final int numTransforms) throws Exception {
        final String reviewsIndexName = "reviews";
        final int numDocs = 100;
        final Instant now = Instant.now();
        createReviewsIndex(
            reviewsIndexName,
            numDocs,
            100,
            TransformIT::getUserIdForRow,
            row -> Instant.ofEpochMilli(now.toEpochMilli() - 1000 * numDocs + 1000 * row).toString(),
            SET_INGEST_TIME_PIPELINE
        );

        List<String> transformIds = new ArrayList<>(numTransforms);
        // Create the chain of transforms. Previous transform's destination index becomes next transform's source index.
        String transformIdPrefix = "my-transform-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT) + "-" + numTransforms + "-";
        for (int i = 0; i < numTransforms; ++i) {
            String transformId = transformIdPrefix + i;
            transformIds.add(transformId);
            // Set up the transform so that its source index is the destination index of the previous transform in the chain.
            // The number of documents is expected to be the same in all the indices.
            String sourceIndex = i == 0 ? reviewsIndexName : transformIds.get(i - 1) + "-dest";
            String destIndex = transformId + "-dest";
            String destReadAlias = destIndex + "-read";
            String destWriteAlias = destIndex + "-write";
            assertFalse(indexExists(destIndex));
            assertFalse(aliasExists(destReadAlias));
            assertFalse(aliasExists(destWriteAlias));

            String transformConfig = createTransformConfig(sourceIndex, destIndex, destReadAlias, destWriteAlias);
            putTransform(transformId, transformConfig, true, RequestOptions.DEFAULT);
        }

        List<String> transformIdsShuffled = new ArrayList<>(transformIds);
        Collections.shuffle(transformIdsShuffled, random());
        // Start all the transforms in random order so that sometimes the transform later in the chain needs to wait for its source index
        // to become available.
        for (String transformId : transformIdsShuffled) {
            startTransform(transformId, RequestOptions.DEFAULT);
        }

        // Give the transforms some time to finish processing. Since the transforms are continuous, we cannot wait for them to be STOPPED.
        assertBusy(() -> {
            // Verify that each transform processed an expected number of documents.
            for (String transformId : transformIds) {
                Map<?, ?> stats = getBasicTransformStats(transformId);
                assertThat(
                    "Stats were: " + stats,
                    XContentMapValues.extractValue(stats, "stats", "documents_processed"),
                    is(equalTo(numDocs))
                );
            }
        }, 60, TimeUnit.SECONDS);

        for (String transformId : transformIds) {
            String destIndex = transformId + "-dest";
            String destReadAlias = destIndex + "-read";
            String destWriteAlias = destIndex + "-write";
            // Verify that the destination index is created.
            assertTrue(indexExists(destIndex));
            // Verify that the destination index aliases are set up.
            assertTrue(aliasExists(destReadAlias));
            assertTrue(aliasExists(destWriteAlias));
        }

        // Stop all the transforms.
        for (String transformId : transformIds) {
            stopTransform(transformId);
        }
        // Delete all the transforms.
        for (String transformId : transformIds) {
            deleteTransform(transformId);
        }
    }

    private static String createTransformConfig(String sourceIndex, String destIndex, String destReadAlias, String destWriteAlias) {
        return Strings.format(
            TRANSFORM_CONFIG_TEMPLATE,
            sourceIndex,
            destIndex,
            SET_INGEST_TIME_PIPELINE,
            destReadAlias,
            destWriteAlias,
            "1s",
            "1s",
            randomBoolean(),
            randomBoolean()
        );
    }
}
