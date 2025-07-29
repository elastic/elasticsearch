/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.gpu;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.gpu.GPUPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class GPUIndexIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(GPUPlugin.class);
    }

    public void testBasic() {
        var settings = Settings.builder().put(indexSettings());
        settings.put("index.number_of_shards", 1);
        settings.put("index.vectors.indexing.use_gpu", "true");
        assertAcked(prepareCreate("foo-index").setSettings(settings.build()).setMapping("""
                "properties": {
                  "my_vector": {
                    "type": "dense_vector",
                    "dims": 5,
                    "similarity": "l2_norm",
                    "index_options": {
                      "type": "hnsw"
                    }
                  }
                }
            """));
        ensureGreen();

        prepareIndex("foo-index").setId("1").setSource("my_vector", new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f }).get();

        // TODO: add more docs...

        ensureGreen();
        refresh();

        // TODO: do some basic search
        // var knn = new KnnSearchBuilder("nested.vector", new float[] { -0.5f, 90.0f, -10f, 14.8f, -156.0f }, 2, 3, null, null);
        // var request = prepareSearch("test").addFetchField("name").setKnnSearch(List.of(knn));
        // assertNoFailuresAndResponse(request, response -> {
        // assertHitCount(response, 2);
        // assertEquals("2", response.getHits().getHits()[0].getId());
        // assertEquals("cat", response.getHits().getHits()[0].field("name").getValue());
        // assertEquals("3", response.getHits().getHits()[1].getId());
        // assertEquals("rat", response.getHits().getHits()[1].field("name").getValue());
        // });
        // }
    }
}
