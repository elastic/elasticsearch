/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class SynonymAnalyzerIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(CommonAnalysisPlugin.class);
    }

    // TODO: update this test when loading from index is ready
    // currently "synonyms_set" option provides only 1 fake synonym rule: "synonym1 => synonym"
    public void testSynonymsLoadedFromIndex() {
        String indexName = "test_index";
        String filterType = randomBoolean() ? "synonym" : "synonym_graph";
        CreateIndexRequestBuilder createIndexRequest = prepareCreate(indexName).setSettings(
            indexSettings(1, 0).put("analysis.analyzer.my_synonym_analyzer.tokenizer", "standard")
                .put("analysis.analyzer.my_synonym_analyzer.filter", "my_synonym_filter")
                .put("analysis.filter.my_synonym_filter.type", filterType)
                .put("analysis.filter.my_synonym_filter.updateable", "true")
                .put("analysis.filter.my_synonym_filter.synonyms_set", "synonyms_set1")
        ).setMapping("field", "type=text,analyzer=standard,search_analyzer=my_synonym_analyzer");
        assertAcked(createIndexRequest);
        ensureGreen(indexName);

        client().prepareIndex(indexName).setId("1").setSource("field", "synonym").get();
        assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().actionGet());

        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchQuery("field", "synonym1")).get();
        assertHitCount(response, 1L);
    }
}
