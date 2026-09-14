/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SliceKnnDiskBBQIT extends ESIntegTestCase {

    @Before
    public void resetLicensing() {
        enableLicensing();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateDiskBBQ.class);
    }

    public void testKnnSearchSlices() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final int dimensions = randomIntBetween(12, 128);
        final int numDocs = randomIntBetween(20, 200);
        final int numSlices = randomIntBetween(3, 8);
        final String indexName = "slice-knn-bbq-all-slices";

        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.shard.check_on_startup", "false")
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
                    .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
            ).setMapping(String.format(Locale.ROOT, """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": %d,
                      "index": true,
                      "similarity": "dot_product",
                      "index_options": {
                        "type": "bbq_disk",
                        "bits": 4
                      }
                    }
                  }
                }""", dimensions))
        );
        ensureGreen(indexName);

        String[] docSliceIds = new String[numDocs];
        int[] docsPerSlice = new int[numSlices];
        int numIndexBatches = randomIntBetween(3, 8);
        for (int batch = 0, docId = 0; batch < numIndexBatches && docId < numDocs; batch++) {
            int batchEndDoc = numDocs * (batch + 1) / numIndexBatches;
            BulkRequestBuilder bulk = client().prepareBulk();
            for (; docId < batchEndDoc; docId++) {
                int slice = between(0, numSlices - 1);
                String sliceId = "s" + slice;
                docSliceIds[docId] = sliceId;
                docsPerSlice[slice]++;
                bulk.add(
                    new IndexRequest(indexName).id(Integer.toString(docId))
                        .source("vector", axisVector(dimensions, docId))
                        .routing(sliceId)
                        .setRoutingFromSlice(true)
                );
            }
            BulkResponse bulkResponse = bulk.get();
            assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
            flushAndRefresh(indexName);
        }

        if (randomBoolean()) {
            // InternalEngine tombstones: flush first so vectors stay on disk, then delete so liveDocs mark them.
            int deleteCount = randomIntBetween(1, Math.max(1, numDocs / 3));
            Set<Integer> docsToDelete = new HashSet<>();
            while (docsToDelete.size() < deleteCount) {
                docsToDelete.add(between(0, numDocs - 1));
            }
            BulkRequestBuilder deleteBulk = client().prepareBulk();
            for (int deletedDocId : docsToDelete) {
                deleteBulk.add(
                    new DeleteRequest(indexName, Integer.toString(deletedDocId)).routing(docSliceIds[deletedDocId])
                        .setRoutingFromSlice(true)
                );
                docsPerSlice[Integer.parseInt(docSliceIds[deletedDocId].substring(1))]--;
            }
            BulkResponse deleteResponse = deleteBulk.get();
            assertFalse(deleteResponse.buildFailureMessage(), deleteResponse.hasFailures());
            flushAndRefresh(indexName);
            // Tombstones and their on-disk vector data survive merges; exercise that merge path often.
            if (randomBoolean()) {
                assertNoFailures(client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get());
            }
        } else if (randomBoolean()) {
            assertNoFailures(client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        }

        for (int slice = 0; slice < numSlices; slice++) {
            final int expectedSliceDocs = docsPerSlice[slice];
            final int k = Math.max(1, expectedSliceDocs);
            final int numCandidates = Math.min(
                KnnSearchBuilder.NUM_CANDS_LIMIT,
                Math.max(k, Math.round(KnnSearchBuilder.NUM_CANDS_MULTIPLICATIVE_FACTOR * k))
            );
            final KnnSearchBuilder knn = new KnnSearchBuilder("vector", axisVector(dimensions, 0), k, numCandidates, 100f, null, null);
            final SearchRequestBuilder search = prepareSearch(indexName).setKnnSearch(List.of(knn)).setSize(k).setTrackTotalHits(true);
            search.request().searchSlice("s" + slice);

            assertResponse(search, response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo((long) expectedSliceDocs));
                assertThat(response.getHits().getHits().length, equalTo(expectedSliceDocs));
            });
        }
    }

    private static float[] axisVector(int dimensions, int axis) {
        float[] vector = new float[dimensions];
        vector[Math.floorMod(axis, dimensions)] = 1.0f;
        return vector;
    }
}
