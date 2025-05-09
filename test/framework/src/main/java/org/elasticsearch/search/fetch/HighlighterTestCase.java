/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FastVectorHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightPhase;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.search.fetch.subphase.highlight.PlainHighlighter;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HighlighterTestCase extends MapperServiceTestCase {

    protected Map<String, Highlighter> getHighlighters() {
        return Map.of(
            "unified",
            new DefaultHighlighter(),
            "fvh",
            new FastVectorHighlighter(getIndexSettings()),
            "plain",
            new PlainHighlighter()
        );
    }

    /**
     * Runs the highlight phase for a search over a specific document
     * @param mapperService the Mappings to use for highlighting
     * @param doc           a parsed document to highlight
     * @param search        the search to highlight
     */
    protected final Map<String, HighlightField> highlight(MapperService mapperService, ParsedDocument doc, SearchSourceBuilder search)
        throws IOException {
        Map<String, HighlightField> highlights = new HashMap<>();
        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {
            SearchExecutionContext context = createSearchExecutionContext(
                mapperService,
                newSearcher(new NoStoredFieldsFilterDirectoryReader(ir))
            );
            HighlightPhase highlightPhase = new HighlightPhase(getHighlighters());
            FetchSubPhaseProcessor processor = highlightPhase.getProcessor(fetchContext(context, search));
            Map<String, List<Object>> storedFields = storedFields(processor.storedFieldsSpec(), doc);
            Source source = Source.fromBytes(doc.source());
            FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
                SearchHit.unpooled(0, "id"),
                ir.leaves().get(0),
                0,
                storedFields,
                source,
                null
            );
            processor.process(hitContext);
            highlights.putAll(hitContext.hit().getHighlightFields());
        });
        return highlights;
    }

    private static Map<String, List<Object>> storedFields(StoredFieldsSpec spec, ParsedDocument doc) {
        Map<String, List<Object>> storedFields = new HashMap<>();
        for (String field : spec.requiredStoredFields()) {
            List<Object> values = storedFields.computeIfAbsent(field, f -> new ArrayList<>());
            for (IndexableField f : doc.rootDoc().getFields(field)) {
                values.add(f.stringValue());
            }
        }
        return storedFields;
    }

    /**
     * Given a set of highlights, assert that any particular field has the expected fragments
     */
    protected static void assertHighlights(Map<String, HighlightField> highlights, String field, String... fragments) {
        assertNotNull("No highlights reported for field [" + field + "]", highlights.get(field));
        HighlightField highlightField = highlights.get(field);
        List<String> actualFragments = Arrays.stream(highlightField.fragments()).map(Text::toString).collect(Collectors.toList());
        List<String> expectedFragments = List.of(fragments);
        assertEquals(expectedFragments, actualFragments);
    }

    private static FetchContext fetchContext(SearchExecutionContext context, SearchSourceBuilder search) throws IOException {
        FetchContext fetchContext = mock(FetchContext.class);
        when(fetchContext.highlight()).thenReturn(search.highlighter().build(context));
        when(fetchContext.parsedQuery()).thenReturn(new ParsedQuery(search.query().toQuery(context)));
        when(fetchContext.getSearchExecutionContext()).thenReturn(context);
        when(fetchContext.sourceLoader()).thenReturn(context.newSourceLoader(false));
        return fetchContext;
    }

    // Wraps a DirectoryReader and ensures that we don't load stored fields from it. For highlighting,
    // stored field access should all be done by the FetchPhase and the highlighter subphases should
    // only be retrieving them from the hit context
    private static class NoStoredFieldsFilterDirectoryReader extends FilterDirectoryReader {

        NoStoredFieldsFilterDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {

                        @Override
                        public StoredFields storedFields() throws IOException {
                            throw new AssertionError("Called Stored Fields!");
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return in.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return in.getReaderCacheHelper();
                        }
                    };
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new NoStoredFieldsFilterDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
