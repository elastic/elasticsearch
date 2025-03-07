/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Base test case for subclasses of MappedFieldType */
public abstract class FieldTypeTestCase extends ESTestCase {

    public static final SearchExecutionContext MOCK_CONTEXT = createMockSearchExecutionContext(true);
    public static final SearchExecutionContext MOCK_CONTEXT_DISALLOW_EXPENSIVE = createMockSearchExecutionContext(false);

    protected SearchExecutionContext randomMockContext() {
        return randomFrom(MOCK_CONTEXT, MOCK_CONTEXT_DISALLOW_EXPENSIVE);
    }

    private static SearchExecutionContext createMockSearchExecutionContext(boolean allowExpensiveQueries) {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(allowExpensiveQueries);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        SearchLookup searchLookup = mock(SearchLookup.class);
        when(searchExecutionContext.lookup()).thenReturn(searchLookup);
        when(searchExecutionContext.indexVersionCreated()).thenReturn(IndexVersion.current());
        return searchExecutionContext;
    }

    public static List<?> fetchSourceValue(MappedFieldType fieldType, Object sourceValue) throws IOException {
        return fetchSourceValue(fieldType, sourceValue, null);
    }

    public static List<?> fetchSourceValue(MappedFieldType fieldType, Object sourceValue, String format) throws IOException {
        String field = fieldType.name();
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = fieldType.valueFetcher(searchExecutionContext, format);
        Source source = Source.fromMap(Collections.singletonMap(field, sourceValue), randomFrom(XContentType.values()));
        return fetcher.fetchValues(source, -1, new ArrayList<>());
    }

    public static List<?> fetchDocValues(MappedFieldType fieldType, Supplier<Document> documentSupplier) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        try (Directory dir = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc)) {
            iw.addDocument(documentSupplier.get());
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
                SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
                when(searchExecutionContext.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH)).thenReturn(
                    fieldType.fielddataBuilder(null).build(null, null)
                );
                ValueFetcher valueFetcher = fieldType.valueFetcher(searchExecutionContext, null);
                valueFetcher.setNextReader(context);
                return valueFetcher.fetchValues(null, 0, new ArrayList<>());
            }
        }
    }

    public static List<?> fetchSourceValues(MappedFieldType fieldType, Object... values) throws IOException {
        String field = fieldType.name();
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = fieldType.valueFetcher(searchExecutionContext, null);
        Source source = Source.fromMap(Collections.singletonMap(field, List.of(values)), randomFrom(XContentType.values()));
        return fetcher.fetchValues(source, -1, new ArrayList<>());
    }

    public static List<?> fetchStoredValue(MappedFieldType fieldType, List<Object> storedValues, String format) throws IOException {
        String field = fieldType.name();
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        SearchLookup searchLookup = mock(SearchLookup.class);
        LeafSearchLookup leafSearchLookup = mock(LeafSearchLookup.class);
        LeafStoredFieldsLookup leafStoredFieldsLookup = mock(LeafStoredFieldsLookup.class);
        FieldLookup fieldLookup = mock(FieldLookup.class);
        when(searchExecutionContext.lookup()).thenReturn(searchLookup);
        when(searchLookup.getLeafSearchLookup(null)).thenReturn(leafSearchLookup);
        when(leafSearchLookup.fields()).thenReturn(leafStoredFieldsLookup);
        when(leafStoredFieldsLookup.get(field)).thenReturn(fieldLookup);
        when(fieldLookup.getValues()).thenReturn(storedValues);

        ValueFetcher fetcher = fieldType.valueFetcher(searchExecutionContext, format);
        fetcher.setNextReader(null);
        return fetcher.fetchValues(null, -1, new ArrayList<>());
    }

    public void testFieldHasValue() {
        MappedFieldType fieldType = getMappedFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName("field") });
        assertTrue(fieldType.fieldHasValue(fieldInfos));
    }

    public void testFieldHasValueWithEmptyFieldInfos() {
        MappedFieldType fieldType = getMappedFieldType();
        assertFalse(fieldType.fieldHasValue(FieldInfos.EMPTY));
    }

    public MappedFieldType getMappedFieldType() {
        return new MappedFieldType("field", false, false, false, TextSearchInfo.NONE, Collections.emptyMap()) {

            @Override
            public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
                return null;
            }

            @Override
            public String typeName() {
                return null;
            }

            @Override
            public Query termQuery(Object value, SearchExecutionContext context) {
                return null;
            }
        };
    }

    public FieldInfo getFieldInfoWithName(String name) {
        return new FieldInfo(
            name,
            1,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            1,
            1,
            1,
            1,
            VectorEncoding.BYTE,
            VectorSimilarityFunction.COSINE,
            randomBoolean(),
            false
        );
    }
}
