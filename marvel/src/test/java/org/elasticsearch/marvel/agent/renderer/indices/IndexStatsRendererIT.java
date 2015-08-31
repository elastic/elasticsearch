/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.hamcrest.Matchers.*;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IndexStatsRendererIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put(MarvelSettings.STARTUP_DELAY, "1s")
                .put(MarvelSettings.INTERVAL, "30s")
                .put(MarvelSettings.COLLECTORS, IndexStatsCollector.NAME)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LicensePlugin.class, MarvelPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Test
    public void testIndexStats() throws Exception {
        final int nbIndices = randomIntBetween(1, 5);
        for (int i = 0; i < nbIndices; i++) {
            createIndex("test" + i);
        }

        final long[] nbDocsPerIndex = new long[nbIndices];
        for (int i = 0; i < nbIndices; i++) {
            nbDocsPerIndex[i] = randomIntBetween(1, 50);
            for (int j = 0; j < nbDocsPerIndex[i]; j++) {
                client().prepareIndex("test" + i, "type1").setSource("num", i).get();
            }
        }
        refresh();

        logger.debug("--> wait for index stats collector to collect stat for each index");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < nbIndices; i++) {
                    CountResponse count = client().prepareCount()
                                                    .setTypes(IndexStatsCollector.TYPE)
                                                    .setQuery(QueryBuilders.termQuery("index_stats.index", "test" + i))
                                                    .get();
                    assertThat(count.getCount(), greaterThan(0L));
                }
            }
        });

        logger.debug("--> get the list of filters used by the renderer");
        String[] filters = IndexStatsRenderer.FILTERS;

        logger.debug("--> checking that every document contains the expected fields");
        SearchResponse response = client().prepareSearch().setTypes(IndexStatsCollector.TYPE).get();
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }
    }

    /**
     * Checks if a field exist in a map of values. If the field contains a dot like 'foo.bar'
     * it checks that 'foo' exists in the map of values and that it points to a sub-map. Then
     * it recurses to check if 'bar' exists in the sub-map.
     */
    private void assertContains(String field, Map<String, Object> values) {
        assertNotNull(field);
        assertNotNull(values);

        int point = field.indexOf('.');
        if (point > -1) {
            assertThat(point, allOf(greaterThan(0), lessThan(field.length())));

            String segment = field.substring(0, point);
            assertTrue(Strings.hasText(segment));

            Object value = values.get(segment);
            assertNotNull(value);

            String next = field.substring(point + 1);
            if (next.length() > 0) {
                assertTrue(value instanceof Map);
                assertContains(next, (Map<String, Object>) value);
            } else {
                assertFalse(value instanceof Map);
            }
        } else {
            assertNotNull(values.get(field));
        }
    }
}
