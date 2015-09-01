/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsMarvelDoc;
import org.elasticsearch.marvel.agent.renderer.Renderer;
import org.elasticsearch.marvel.agent.renderer.RendererTestUtils;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.junit.Test;

public class IndicesStatsRendererTests extends ESSingleNodeTestCase {

    private static final String SAMPLE_FILE = "/samples/marvel_indices_stats.json";

    @Test
    public void testIndexStatsRenderer() throws Exception {
        createIndex("index-0");

        logger.debug("--> retrieving indices stats response");
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().get();

        logger.debug("--> creating the indices stats marvel document");
        IndicesStatsMarvelDoc marvelDoc = new IndicesStatsMarvelDoc("test", "marvel_indices_stats", 1437580442979L, indicesStats);

        logger.debug("--> rendering the document");
        Renderer renderer = new IndicesStatsRenderer();
        String result = RendererTestUtils.renderAsJSON(marvelDoc, renderer);

        logger.debug("--> loading sample document from file {}", SAMPLE_FILE);
        String expected = StreamsUtils.copyToStringFromClasspath(SAMPLE_FILE);

        logger.debug("--> comparing both documents, they must have the same structure");
        RendererTestUtils.assertJSONStructure(result, expected);
    }
}
