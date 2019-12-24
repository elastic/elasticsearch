/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.KeyedFlatObjectFieldType;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class KeyedFlatObjectFieldTypeTests extends FieldTypeTestCase {

    @Before
    public void setupProperties() {
        addModifier(new Modifier("split_queries_on_whitespace", true) {
            @Override
            public void modify(MappedFieldType type) {
                KeyedFlatObjectFieldType ft = (KeyedFlatObjectFieldType) type;
                ft.setSplitQueriesOnWhitespace(!ft.splitQueriesOnWhitespace());
            }
        });
    }

    @Override
    protected KeyedFlatObjectFieldType createDefaultFieldType() {
        return new KeyedFlatObjectFieldType("key");
    }

    public void testIndexedValueForSearch() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        BytesRef keywordValue = ft.indexedValueForSearch("value");
        assertEquals(new BytesRef("key\0value"), keywordValue);

        BytesRef doubleValue = ft.indexedValueForSearch(2.718);
        assertEquals(new BytesRef("key\0" + "2.718"), doubleValue);

        BytesRef booleanValue = ft.indexedValueForSearch(true);
        assertEquals(new BytesRef("key\0true"), booleanValue);
    }

    public void testTermQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new TermQuery(new Term("field", "key\0value"));
        assertEquals(expected, ft.termQuery("value", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ft.termQuery("field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

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
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new PrefixQuery(new Term("field", "key\0"));
        assertEquals(expected, ft.existsQuery(null));
    }

    public void testPrefixQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new PrefixQuery(new Term("field", "key\0val"));
        assertEquals(expected, ft.prefixQuery("val", MultiTermQuery.CONSTANT_SCORE_REWRITE, null));
    }

    public void testFuzzyQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.fuzzyQuery("valuee", Fuzziness.fromEdits(2), 1, 50, true));
        assertEquals("[fuzzy] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testRangeQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        TermRangeQuery expected = new TermRangeQuery("field",
            new BytesRef("key\0lower"),
            new BytesRef("key\0upper"), false, false);
        assertEquals(expected, ft.rangeQuery("lower", "upper", false, false, null));

        expected = new TermRangeQuery("field",
            new BytesRef("key\0lower"),
            new BytesRef("key\0upper"), true, true);
        assertEquals(expected, ft.rangeQuery("lower", "upper", true, true, null));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            ft.rangeQuery("lower", null, false, false, null));
        assertEquals("[range] queries on keyed [flattened] fields must include both an upper and a lower bound.",
            e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () ->
            ft.rangeQuery(null, "upper", false, false, null));
        assertEquals("[range] queries on keyed [flattened] fields must include both an upper and a lower bound.",
            e.getMessage());
    }

    public void testRegexpQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.regexpQuery("valu*", 0, 10, null, null));
        assertEquals("[regexp] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }

    public void testWildcardQuery() {
        KeyedFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
            () -> ft.wildcardQuery("valu*", null, null));
        assertEquals("[wildcard] queries are not currently supported on keyed [flattened] fields.", e.getMessage());
    }
}
