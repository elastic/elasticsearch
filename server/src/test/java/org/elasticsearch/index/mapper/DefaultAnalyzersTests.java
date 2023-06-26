/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DefaultAnalyzersTests extends MapperServiceTestCase {

    private boolean setDefaultSearchAnalyzer;
    private boolean setDefaultSearchQuoteAnalyzer;

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(AnalysisRegistry.DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()));
        if (setDefaultSearchAnalyzer) {
            analyzers.put(
                AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME,
                new NamedAnalyzer("default_search", AnalyzerScope.INDEX, new StandardAnalyzer())
            );
        }
        if (setDefaultSearchQuoteAnalyzer) {
            analyzers.put(
                AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME,
                new NamedAnalyzer("default_search_quote", AnalyzerScope.INDEX, new StandardAnalyzer())
            );
        }
        analyzers.put("configured", new NamedAnalyzer("configured", AnalyzerScope.INDEX, new StandardAnalyzer()));
        return IndexAnalyzers.of(analyzers);
    }

    public void testDefaultSearchAnalyzer() throws IOException {
        {
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default", ft.getTextSearchInfo().searchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search", ft.getTextSearchInfo().searchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchAnalyzer().name());
        }
        {
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchAnalyzer().name());
        }

    }

    public void testDefaultSearchQuoteAnalyzer() throws IOException {
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search_quote", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = false;
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("default_search_quote", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = true;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_quote_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
        {
            setDefaultSearchQuoteAnalyzer = true;
            setDefaultSearchAnalyzer = false;
            MapperService ms = createMapperService(fieldMapping(b -> b.field("type", "text").field("search_analyzer", "configured")));
            MappedFieldType ft = ms.fieldType("field");
            assertEquals("configured", ft.getTextSearchInfo().searchQuoteAnalyzer().name());
        }
    }

}
