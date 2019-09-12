/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper.RootFlatObjectFieldType;
import org.junit.Before;

public class RootFlatObjectFieldTypeTests extends FieldTypeTestCase {

    @Before
    public void setupProperties() {
        addModifier(new Modifier("split_queries_on_whitespace", true) {
            @Override
            public void modify(MappedFieldType type) {
                RootFlatObjectFieldType ft = (RootFlatObjectFieldType) type;
                ft.setSplitQueriesOnWhitespace(!ft.splitQueriesOnWhitespace());
            }
        });
    }

    @Override
    protected RootFlatObjectFieldType createDefaultFieldType() {
        return new RootFlatObjectFieldType();
    }

    public void testValueForDisplay() {
        RootFlatObjectFieldType ft = createDefaultFieldType();

        String fieldValue = "{ \"key\": \"value\" }";
        BytesRef storedValue = new BytesRef(fieldValue);
        assertEquals(fieldValue, ft.valueForDisplay(storedValue));
    }

    public void testTermQuery() {
        RootFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new TermQuery(new Term("field", "value"));
        assertEquals(expected, ft.termQuery("value", null));

        ft.setIndexOptions(IndexOptions.NONE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ft.termQuery("field", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testExistsQuery() {
        RootFlatObjectFieldType ft = new RootFlatObjectFieldType();
        ft.setName("field");
        assertEquals(
            new TermQuery(new Term(FieldNamesFieldMapper.NAME, new BytesRef("field"))),
            ft.existsQuery(null));

        ft.setHasDocValues(true);
        assertEquals(new DocValuesFieldExistsQuery("field"), ft.existsQuery(null));
    }

    public void testFuzzyQuery() {
        RootFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new FuzzyQuery(new Term("field", "value"), 2, 1, 50, true);
        Query actual = ft.fuzzyQuery("value", Fuzziness.fromEdits(2), 1, 50, true);
        assertEquals(expected, actual);
    }

    public void testRangeQuery() {
        RootFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        TermRangeQuery expected = new TermRangeQuery("field",
            new BytesRef("lower"),
            new BytesRef("upper"), false, false);
        assertEquals(expected, ft.rangeQuery("lower", "upper", false, false, null));

        expected = new TermRangeQuery("field",
            new BytesRef("lower"),
            new BytesRef("upper"), true, true);
        assertEquals(expected, ft.rangeQuery("lower", "upper", true, true, null));
    }

    public void testRegexpQuery() {
        RootFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new RegexpQuery(new Term("field", "val.*"));
        Query actual = ft.regexpQuery("val.*", 0, 10, null, null);
        assertEquals(expected, actual);
    }

    public void testWildcardQuery() {
        RootFlatObjectFieldType ft = createDefaultFieldType();
        ft.setName("field");

        Query expected = new WildcardQuery(new Term("field", new BytesRef("valu*")));
        assertEquals(expected, ft.wildcardQuery("valu*", null, null));
    }
}
