/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReloadableAnalyzerTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ReloadableFilterPlugin.class);
    }

    public void testReloadSearchAnalyzers() throws IOException {
        Settings settings = indexSettings(1, 1).put("index.analysis.analyzer.reloadableAnalyzer.type", "custom")
            .put("index.analysis.analyzer.reloadableAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.reloadableAnalyzer.filter", "myReloadableFilter")
            .build();

        MapperService mapperService = createIndex("test_index", settings).mapperService();
        CompressedXContent mapping = new CompressedXContent(
            BytesReference.bytes(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("_doc")
                    .startObject("properties")
                    .startObject("field")
                    .field("type", "text")
                    .field("analyzer", "simple")
                    .field("search_analyzer", "reloadableAnalyzer")
                    .field("search_quote_analyzer", "stop")
                    .endObject()
                    .startObject("otherField")
                    .field("type", "text")
                    .field("analyzer", "standard")
                    .field("search_analyzer", "simple")
                    .field("search_quote_analyzer", "reloadableAnalyzer")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        mapperService.merge("_doc", mapping, MapperService.MergeReason.MAPPING_UPDATE);
        IndexAnalyzers current = mapperService.getIndexAnalyzers();

        ReloadableCustomAnalyzer originalReloadableAnalyzer = (ReloadableCustomAnalyzer) current.get("reloadableAnalyzer").analyzer();
        TokenFilterFactory[] originalTokenFilters = originalReloadableAnalyzer.getComponents().getTokenFilters();
        assertEquals(1, originalTokenFilters.length);
        assertEquals("myReloadableFilter", originalTokenFilters[0].name());

        // now reload, this should change the tokenfilterFactory inside the analyzer
        mapperService.reloadSearchAnalyzers(getInstanceFromNode(AnalysisRegistry.class), null, false);
        IndexAnalyzers updatedAnalyzers = mapperService.getIndexAnalyzers();
        assertSame(current, updatedAnalyzers);
        assertSame(current.getDefaultIndexAnalyzer(), updatedAnalyzers.getDefaultIndexAnalyzer());
        assertSame(current.getDefaultSearchAnalyzer(), updatedAnalyzers.getDefaultSearchAnalyzer());
        assertSame(current.getDefaultSearchQuoteAnalyzer(), updatedAnalyzers.getDefaultSearchQuoteAnalyzer());

        assertFalse(assertSameContainedFilters(originalTokenFilters, current.get("reloadableAnalyzer")));
        assertFalse(
            assertSameContainedFilters(originalTokenFilters, mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer())
        );
        assertFalse(
            assertSameContainedFilters(
                originalTokenFilters,
                mapperService.fieldType("otherField").getTextSearchInfo().searchQuoteAnalyzer()
            )
        );
    }

    private boolean assertSameContainedFilters(TokenFilterFactory[] originalTokenFilter, NamedAnalyzer updatedAnalyzer) {
        ReloadableCustomAnalyzer updatedReloadableAnalyzer = (ReloadableCustomAnalyzer) updatedAnalyzer.analyzer();
        TokenFilterFactory[] newTokenFilters = updatedReloadableAnalyzer.getComponents().getTokenFilters();
        assertEquals(originalTokenFilter.length, newTokenFilters.length);
        int i = 0;
        for (TokenFilterFactory tf : newTokenFilters) {
            assertEquals(originalTokenFilter[i].name(), tf.name());
            if (originalTokenFilter[i] != tf) {
                return false;
            }
            i++;
        }
        return true;
    }

    public static final class ReloadableFilterPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return Collections.singletonMap("myReloadableFilter", (indexSettings, environment, name, settings) -> new TokenFilterFactory() {
                @Override
                public String name() {
                    return "myReloadableFilter";
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    return tokenStream;
                }

                @Override
                public AnalysisMode getAnalysisMode() {
                    return AnalysisMode.SEARCH_TIME;
                }
            });
        }
    }
}
