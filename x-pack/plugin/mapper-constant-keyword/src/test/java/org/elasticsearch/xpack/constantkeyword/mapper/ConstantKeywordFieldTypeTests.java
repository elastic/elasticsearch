/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper.ConstantKeywordFieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ConstantKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("foo", null));
        assertEquals(new MatchAllDocsQuery(), ft.termQueryCaseInsensitive("fOo", null));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("bar", null));
        assertEquals(new MatchNoDocsQuery(), ft.termQueryCaseInsensitive("bAr", null));
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(new MatchNoDocsQuery(), bar.termQuery("foo", null));
        assertEquals(new MatchNoDocsQuery(), bar.termQueryCaseInsensitive("fOo", null));
    }

    public void testTermsQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(new MatchNoDocsQuery(), bar.termsQuery(Collections.singletonList("foo"), null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Collections.singletonList("foo"), null));
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery(Arrays.asList("bar", "foo", "quux"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.emptyList(), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Collections.singletonList("bar"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery(Arrays.asList("bar", "quux"), null));
    }

    public void testWildcardQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(new MatchNoDocsQuery(), bar.wildcardQuery("f*o", null, false, null));
        assertEquals(new MatchNoDocsQuery(), bar.wildcardQuery("F*o", null, true, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("f*o", null, false, null));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("F*o", null, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("b*r", null, false, null));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("B*r", null, true, null));
    }

    public void testPrefixQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(new MatchNoDocsQuery(), bar.prefixQuery("fo", null, false, null));
        assertEquals(new MatchNoDocsQuery(), bar.prefixQuery("fO", null, true, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("fo", null, false, null));
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("fO", null, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("ba", null, false, null));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("Ba", null, true, null));
    }

    public void testExistsQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(new MatchNoDocsQuery(), none.existsQuery(null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.existsQuery(null));
    }

    public void testRangeQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(new MatchNoDocsQuery(), none.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), none.rangeQuery(null, "foo", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), none.rangeQuery("foo", null, randomBoolean(), randomBoolean(), null, null, null, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("foo", null, true, randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("foo", null, false, randomBoolean(), null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery(null, "foo", randomBoolean(), true, null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(null, "foo", randomBoolean(), false, null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("abc", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("abc", "def", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("mno", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
    }

    public void testFuzzyQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(new MatchNoDocsQuery(), none.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foobar");
        assertEquals(new MatchAllDocsQuery(), ft.fuzzyQuery("foobaz", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        assertEquals(new MatchNoDocsQuery(), ft.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
    }

    public void testRegexpQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(new MatchNoDocsQuery(), none.regexpQuery("f..o", RegExp.ALL, 0, 10, null, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("f.o", RegExp.ALL, 0, 10, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.regexpQuery("f..o", RegExp.ALL, 0, 10, null, null));
    }

    public void testFetchValue() throws Exception {
        MappedFieldType fieldType = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", null);
        ValueFetcher fetcher = fieldType.valueFetcher(null, null);

        SourceLookup missingValueLookup = new SourceLookup();
        SourceLookup nullValueLookup = new SourceLookup();
        nullValueLookup.setSource(Collections.singletonMap("field", null));

        List<Object> ignoredValues = new ArrayList<>();
        assertTrue(fetcher.fetchValues(missingValueLookup, ignoredValues).isEmpty());
        assertTrue(fetcher.fetchValues(nullValueLookup, ignoredValues).isEmpty());

        MappedFieldType valued = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", "foo");
        fetcher = valued.valueFetcher(null, null);

        assertEquals(List.of("foo"), fetcher.fetchValues(missingValueLookup, ignoredValues));
        assertEquals(List.of("foo"), fetcher.fetchValues(nullValueLookup, ignoredValues));
    }
}
