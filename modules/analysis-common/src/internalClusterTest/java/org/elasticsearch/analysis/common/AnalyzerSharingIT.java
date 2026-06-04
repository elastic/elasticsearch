/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * End-to-end coverage of node-level analyzer sharing: two indices created through the real index
 * lifecycle that declare an identical custom analyzer recipe must resolve to the very same
 * {@link NamedAnalyzer} instance, and a shared analyzer must outlive deletion of one of its
 * sharers (the reference-counted cache only closes it once the last referencing index is gone).
 */
public class AnalyzerSharingIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(CommonAnalysisPlugin.class);
    }

    private static Settings recipe(String stopword) {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", stopword)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "my_stop")
            .build();
    }

    public void testIdenticalRecipesShareAnalyzerInstance() {
        IndexService a = createIndex("index_a", recipe("the"));
        IndexService b = createIndex("index_b", recipe("the"));
        assertSame(
            "two indices with an identical analyzer recipe must share one analyzer instance",
            a.getIndexAnalyzers().get("a"),
            b.getIndexAnalyzers().get("a")
        );
    }

    public void testDifferentRecipesDoNotShare() {
        IndexService a = createIndex("index_a", recipe("the"));
        IndexService c = createIndex("index_c", recipe("and"));
        assertNotSame(
            "indices whose analyzer recipe differs must not share an analyzer",
            a.getIndexAnalyzers().get("a"),
            c.getIndexAnalyzers().get("a")
        );
    }

    public void testSharedAnalyzerSurvivesDeletionOfOneSharer() throws IOException {
        createIndex("index_a", recipe("the"));
        IndexService b = createIndex("index_b", recipe("the"));
        NamedAnalyzer shared = b.getIndexAnalyzers().get("a");

        assertAcked(indicesAdmin().prepareDelete("index_a"));

        // index_b still references the shared analyzer, so it must not have been closed when
        // index_a (the other sharer) was deleted — exercising the refcounted release path.
        assertSame(shared, b.getIndexAnalyzers().get("a"));
        try (TokenStream ts = shared.tokenStream("f", "The Quick Brown Fox")) {
            ts.reset();
            while (ts.incrementToken()) {
            }
            ts.end();
        }
    }
}
