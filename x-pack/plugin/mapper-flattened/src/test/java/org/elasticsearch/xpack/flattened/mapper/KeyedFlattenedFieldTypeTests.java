/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.flattened.mapper;

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
import org.elasticsearch.xpack.flattened.mapper.FlattenedFieldMapper.KeyedFlattenedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KeyedFlattenedFieldTypeTests extends FieldTypeTestCase {

    private static KeyedFlattenedFieldType createFieldType() {
        return new KeyedFlattenedFieldType("field", true, true, "key", false, Collections.emptyMap());
    }

    public void testIndexedValueForSearch() {
        KeyedFlattenedFieldType ft = createFieldType();

        BytesRef keywordValue = ft.indexedValueForSearch("value");
        assertEquals(new BytesRef("key\0value"), keywordValue);

        BytesRef doubleValue = ft.indexedValueForSearch(2.718);
        assertEquals(new BytesRef("key\0" + "2.718"), doubleValue);

        BytesRef booleanValue = ft.indexedValueForSearch(true);
        assertEquals(new BytesRef("key\0true"), booleanValue);
    }

    public void testTermQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new TermQuery(new Term("field", "key\0value"));
        assertEquals(expected, ft.termQuery("value", null));

        expected = AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "key\0value"));
        assertEquals(expected, ft.termQueryCaseInsensitive("value", null));

        KeyedFlattenedFieldType unsearchable = new KeyedFlattenedFieldType("field", false, true, "key",
            false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> unsearchable.termQuery("field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new TermInSetQuery("field",
            new BytesRef("key\0value1"),
            new BytesRef("key\0value2"));

        List<String> terms = new ArrayList<>();
        terms.add("value1");
        terms.add("value2");
        Query actual = ft.termsQuery(terms, null);

        assertEquals(expected, actual);
    }

    public void testExistsQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new PrefixQuery(new Term("field", "key\0"));
        assertEquals(expected, ft.existsQuery(null));
    }

    public void testPrefixQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        Query expected = new PrefixQuery(new Term("field", "key\0val"));
        assertEquals(expected, ft.prefixQuery("val", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, MOCK_CONTEXT));

        expected = AutomatonQueries.caseInsensitivePrefixQuery(new Term("field", "key\0vAl"));
        assertEquals(expected, ft.prefixQuery("vAl", MultiTermQuery.CONSTANT_SCORE_REWRITE, true, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.prefixQuery("val", MultiTermQuery.CONSTANT_SCORE_REWRITE, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals("[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. " +
                "For optimised prefix queries on text fields please enable [index_prefixes].", ee.getMessage());
    }

    public void testFuzzyQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.fuzzyQuery("value", Fuzziness.fromEdits(2), 1, 50, true, randomMockContext()));
        assertEquals("[fuzzy] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testRangeQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        TermRangeQuery expected = new TermRangeQuery("field",
            new BytesRef("key\0lower"),
            new BytesRef("key\0upper"), false, false);
        assertEquals(expected, ft.rangeQuery("lower", "upper", false, false, MOCK_CONTEXT));

        expected = new TermRangeQuery("field",
            new BytesRef("key\0lower"),
            new BytesRef("key\0upper"), true, true);
        assertEquals(expected, ft.rangeQuery("lower", "upper", true, true, MOCK_CONTEXT));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            ft.rangeQuery("lower", null, false, false, null));
        assertEquals("[range] queries on keyed [flattened] fields must include both an upper and a lower bound.",
            e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            ft.rangeQuery(null, "upper", false, false, MOCK_CONTEXT));
        assertEquals("[range] queries on keyed [flattened] fields must include both an upper and a lower bound.",
            e.getMessage());

        ElasticsearchException ee = expectThrows(ElasticsearchException.class,
                () -> ft.rangeQuery("lower", "upper", false, false, MOCK_CONTEXT_DISALLOW_EXPENSIVE));
        assertEquals("[range] queries on [text] or [keyword] fields cannot be executed when " +
                "'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testRegexpQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.regexpQuery("valu*", 0, 0, 10, null, randomMockContext()));
        assertEquals("[regexp] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testWildcardQuery() {
        KeyedFlattenedFieldType ft = createFieldType();

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.wildcardQuery("valu*", null, false, randomMockContext()));
        assertEquals("[wildcard] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testFetchIsEmpty() throws IOException {
        Map<String, Object> sourceValue = Map.of("key", "value");
        KeyedFlattenedFieldType ft = createFieldType();

        assertEquals(List.of(), fetchSourceValue(ft, sourceValue));
        assertEquals(List.of(), fetchSourceValue(ft, null));
    }
}
