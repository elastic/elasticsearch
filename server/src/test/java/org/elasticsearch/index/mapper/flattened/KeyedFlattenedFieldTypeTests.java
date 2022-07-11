/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedField;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper.KeyedFlattenedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KeyedFlattenedFieldTypeTests extends FieldTypeTestCase {

    private static KeyedFlattenedFieldType createFieldType() {
        return new KeyedFlattenedFieldType("field", true, true, "key", false, Collections.emptyMap());
    }

    public void testIndexedValueForSearch() {
        KeyedFlattenedFieldType ft = createFieldType();

        BytesRef keywordValue = ft.indexedValueForSearch("field", "value");
        assertEquals(new BytesRef("key\0value"), keywordValue);

        BytesRef doubleValue = ft.indexedValueForSearch("field", 2.718);
        assertEquals(new BytesRef("key\0" + "2.718"), doubleValue);

        BytesRef booleanValue = ft.indexedValueForSearch("field", true);
        assertEquals(new BytesRef("key\0true"), booleanValue);
    }

    public void testTermQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new TermQuery(new Term("field", "key\0value"));
        assertEquals(expected, ft.termQuery("field", "value", null));

        expected = AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "key\0value"));
        assertEquals(expected, ft.termQueryCaseInsensitive("field", "value", null));

        KeyedFlattenedFieldType unsearchable = new KeyedFlattenedFieldType("field", false, true, "key", false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("field", "field", null));
        assertEquals("Cannot search on field [" + "field" + "] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new TermInSetQuery("field", new BytesRef("key\0value1"), new BytesRef("key\0value2"));

        List<String> terms = new ArrayList<>();
        terms.add("value1");
        terms.add("value2");
        Query actual = ft.termsQuery("field", terms, null);

        assertEquals(expected, actual);
    }

    public void testExistsQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new PrefixQuery(new Term("field", "key\0"));
        assertEquals(expected, ft.existsQuery("field", null));
    }

    public void testPrefixQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new PrefixQuery(new Term("field", "key\0val"));
        assertEquals(expected, ft.prefixQuery("field", "val", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, MOCK_CONTEXT));

        expected = AutomatonQueries.caseInsensitivePrefixQuery(new Term("field", "key\0vAl"));
        assertEquals(expected, ft.prefixQuery("field", "vAl", MultiTermQuery.CONSTANT_SCORE_REWRITE, true, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.prefixQuery("field", "val", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );
    }

    public void testFuzzyQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.fuzzyQuery("field", "value", Fuzziness.fromEdits(2), 1, 50, true, randomMockContext())
        );
        assertEquals("[fuzzy] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testRangeQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        TermRangeQuery expected = new TermRangeQuery("field", new BytesRef("key\0lower"), new BytesRef("key\0upper"), false, false);
        assertEquals(expected, ft.rangeQuery("field", "lower", "upper", false, false, MOCK_CONTEXT));

        expected = new TermRangeQuery("field", new BytesRef("key\0lower"), new BytesRef("key\0upper"), true, true);
        assertEquals(expected, ft.rangeQuery("field", "lower", "upper", true, true, MOCK_CONTEXT));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery("field", "lower", null, false, false, null)
        );
        assertEquals("[range] queries on keyed [flattened] fields must include both an upper and a lower bound.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> ft.rangeQuery("field", null, "upper", false, false, MOCK_CONTEXT));
        assertEquals("[range] queries on keyed [flattened] fields must include both an upper and a lower bound.", e.getMessage());

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("field", "lower", "upper", false, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.regexpQuery("field", "valu*", 0, 0, 10, null, randomMockContext())
        );
        assertEquals("[regexp] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testWildcardQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> ft.wildcardQuery("field", "valu*", null, false, randomMockContext())
        );
        assertEquals("[wildcard] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testFetchIsEmpty() throws IOException {
        Map<String, Object> sourceValue = Map.of("key", "value");
        MappedField mappedField = new MappedField("field", createFieldType());

        assertEquals(List.of(), fetchSourceValue(mappedField, sourceValue));
        assertEquals(List.of(), fetchSourceValue(mappedField, null));
    }

    public void testFetchSourceValue() throws IOException {
        MappedField mappedField = new MappedField("field", createFieldType());
        Map<String, Object> sourceValue = Map.of("key", "value");

        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath("field.key")).thenReturn(Set.of("field.key"));

        ValueFetcher fetcher = mappedField.valueFetcher(searchExecutionContext, null);
        SourceLookup lookup = new SourceLookup();
        lookup.setSource(Collections.singletonMap("field", sourceValue));

        assertEquals(List.of("value"), fetcher.fetchValues(lookup, new ArrayList<Object>()));
        lookup.setSource(Collections.singletonMap("field", null));
        assertEquals(List.of(), fetcher.fetchValues(lookup, new ArrayList<Object>()));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> mappedField.valueFetcher(searchExecutionContext, "format")
        );
        assertEquals("Field [field.key] of type [flattened] doesn't support formats.", e.getMessage());
    }
}
