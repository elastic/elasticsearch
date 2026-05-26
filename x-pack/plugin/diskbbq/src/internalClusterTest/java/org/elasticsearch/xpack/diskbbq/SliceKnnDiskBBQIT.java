/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.diskbbq;

import org.apache.lucene.tests.util.LuceneTestCase;
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
import java.util.List;

import static org.elasticsearch.license.DiskBBQLicensingIT.enableLicensing;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
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

    public void testSliceSearchUsesSlicedIVFQuery() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        assertAcked(
            prepareCreate("slice-knn-bbq").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.shard.check_on_startup", "false")
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
                    .put(IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.getKey(), true)
            ).setMapping("""
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": 64,
                      "index": true,
                      "similarity": "dot_product",
                      "index_options": {
                        "type": "bbq_disk",
                        "bits": 4
                      }
                    }
                  }
                }""")
        );
        ensureGreen("slice-knn-bbq");

        client().index(new IndexRequest("slice-knn-bbq").id("1").source("vector", axisVector(0)).routing("s1").setRoutingFromSlice(true))
            .actionGet();
        client().index(new IndexRequest("slice-knn-bbq").id("2").source("vector", axisVector(1)).routing("s2").setRoutingFromSlice(true))
            .actionGet();
        refresh("slice-knn-bbq");

        final KnnSearchBuilder knn = new KnnSearchBuilder("vector", axisVector(1), 1, 10, 100f, null, null);
        final SearchRequestBuilder search = prepareSearch("slice-knn-bbq").setKnnSearch(List.of(knn));
        search.request().searchSlice("s1");

        assertResponse(search, response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        });
    }

    private static float[] axisVector(int axis) {
        float[] vector = new float[64];
        vector[axis] = 1.0f;
        return vector;
    }
}
