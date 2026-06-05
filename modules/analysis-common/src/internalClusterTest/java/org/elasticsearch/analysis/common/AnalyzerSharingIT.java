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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

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

    /** Same recipe, but the analyzer carries a different local name in each index. */
    private static Settings recipeNamed(String analyzerName, String stopword) {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", stopword)
            .put("index.analysis.analyzer." + analyzerName + ".tokenizer", "standard")
            .putList("index.analysis.analyzer." + analyzerName + ".filter", "lowercase", "my_stop")
            .build();
    }

    /**
     * Regression test for the shared-analyzer local-name leak: two indices declare a byte-for-byte
     * identical recipe but name the analyzer differently ({@code ana} vs {@code anb}) and use it as a
     * field's analyzer. Sharing keys on the recipe, so both fields are backed by one shared analyzer
     * instance — but the {@link NamedAnalyzer} wrapper handed to each index must keep that index's
     * OWN local name. {@code FieldMapper} serializes the analyzer via {@code NamedAnalyzer.name()}; if
     * index_b's wrapper leaked index_a's name ({@code ana}), index_b's mapping would serialize an
     * analyzer that is not configured in index_b and the round-trip in {@code MapperService}
     * assertSerialization (run under {@code -ea}) would throw at index-creation time. Creating the
     * second index is therefore itself the assertion; the explicit checks below document the intent.
     */
    public void testSharedAnalyzerKeepsPerIndexLocalNameInMappings() throws IOException {
        IndexService a = createIndex("index_a", recipeNamed("ana", "the"), contentFieldMapping("ana"));
        // With the bug present, this create throws: index_b reuses index_a's shared wrapper (named
        // "ana"), its content field serializes analyzer "ana", and re-parsing fails because index_b
        // only configures "anb".
        IndexService b = createIndex("index_b", recipeNamed("anb", "the"), contentFieldMapping("anb"));

        NamedAnalyzer ana = a.getIndexAnalyzers().get("ana");
        NamedAnalyzer anb = b.getIndexAnalyzers().get("anb");
        assertSame("identical recipes must share the underlying analyzer", ana.analyzer(), anb.analyzer());
        assertEquals("index_a's wrapper must keep its own local name", "ana", ana.name());
        assertEquals("index_b's wrapper must keep its own local name, not the shared builder's", "anb", anb.name());

        // The field mappers must serialize each index's own analyzer name, so the mapping round-trips.
        String mappingSourceA = a.mapperService().documentMapper().mappingSource().toString();
        String mappingSourceB = b.mapperService().documentMapper().mappingSource().toString();
        assertThat(mappingSourceA, containsString("\"analyzer\":\"ana\""));
        assertThat(mappingSourceB, containsString("\"analyzer\":\"anb\""));
    }

    /** A {@code content} text field whose analyzer is the custom analyzer named {@code analyzerName}. */
    private static XContentBuilder contentFieldMapping(String analyzerName) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("content")
            .field("type", "text")
            .field("analyzer", analyzerName)
            .endObject()
            .endObject()
            .endObject();
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
