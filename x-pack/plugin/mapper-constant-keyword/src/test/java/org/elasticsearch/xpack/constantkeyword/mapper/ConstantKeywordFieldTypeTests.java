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
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.termQuery("f", "foo", null));
        assertEquals(new MatchAllDocsQuery(), ft.termQueryCaseInsensitive("f", "fOo", null));
        assertEquals(new MatchNoDocsQuery(), ft.termQuery("f", "bar", null));
        assertEquals(new MatchNoDocsQuery(), ft.termQueryCaseInsensitive("f", "bAr", null));
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("bar");
        assertEquals(new MatchNoDocsQuery(), bar.termQuery("f", "foo", null));
        assertEquals(new MatchNoDocsQuery(), bar.termQueryCaseInsensitive("f", "fOo", null));
    }

    public void testTermsQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("bar");
        assertEquals(new MatchNoDocsQuery(), bar.termsQuery("f", Collections.singletonList("foo"), null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery("f", Collections.singletonList("foo"), null));
        assertEquals(new MatchAllDocsQuery(), ft.termsQuery("f", Arrays.asList("bar", "foo", "quux"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery("f", Collections.emptyList(), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery("f", Collections.singletonList("bar"), null));
        assertEquals(new MatchNoDocsQuery(), ft.termsQuery("f", Arrays.asList("bar", "quux"), null));
    }

    public void testWildcardQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("bar");
        assertEquals(new MatchNoDocsQuery(), bar.wildcardQuery("f", "f*o", null, false, null));
        assertEquals(new MatchNoDocsQuery(), bar.wildcardQuery("f", "F*o", null, true, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("f", "f*o", null, false, null));
        assertEquals(new MatchAllDocsQuery(), ft.wildcardQuery("f", "F*o", null, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("f", "b*r", null, false, null));
        assertEquals(new MatchNoDocsQuery(), ft.wildcardQuery("f", "B*r", null, true, null));
    }

    public void testPrefixQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("bar");
        assertEquals(new MatchNoDocsQuery(), bar.prefixQuery("f", "fo", null, false, null));
        assertEquals(new MatchNoDocsQuery(), bar.prefixQuery("f", "fO", null, true, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("f", "fo", null, false, null));
        assertEquals(new MatchAllDocsQuery(), ft.prefixQuery("f", "fO", null, true, null));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("f", "ba", null, false, null));
        assertEquals(new MatchNoDocsQuery(), ft.prefixQuery("f", "Ba", null, true, null));
    }

    public void testExistsQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType(null);
        assertEquals(new MatchNoDocsQuery(), none.existsQuery("f", null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.existsQuery("f", null));
    }

    public void testRangeQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType(null);
        assertEquals(new MatchNoDocsQuery(), none.rangeQuery("f", null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), none.rangeQuery("f", null, "foo", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), none.rangeQuery("f", "foo", null, randomBoolean(), randomBoolean(), null, null, null, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("f", null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("f", "foo", null, true, randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("f", "foo", null, false, randomBoolean(), null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("f", null, "foo", randomBoolean(), true, null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("f", null, "foo", randomBoolean(), false, null, null, null, null));
        assertEquals(new MatchAllDocsQuery(), ft.rangeQuery("f", "abc", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("f", "abc", "def", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery("f", "mno", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
    }

    public void testFuzzyQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType(null);
        assertEquals(new MatchNoDocsQuery(), none.fuzzyQuery("f", "fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foobar");
        assertEquals(new MatchAllDocsQuery(), ft.fuzzyQuery("f", "foobaz", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        assertEquals(new MatchNoDocsQuery(), ft.fuzzyQuery("f", "fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
    }

    public void testRegexpQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType(null);
        assertEquals(new MatchNoDocsQuery(), none.regexpQuery("f", "f..o", RegExp.ALL, 0, 10, null, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("foo");
        assertEquals(new MatchAllDocsQuery(), ft.regexpQuery("f", "f.o", RegExp.ALL, 0, 10, null, null));
        assertEquals(new MatchNoDocsQuery(), ft.regexpQuery("f", "f..o", RegExp.ALL, 0, 10, null, null));
    }

    public void testFetchValue() throws Exception {
        MappedFieldType fieldType = new ConstantKeywordFieldMapper.ConstantKeywordFieldType(null);
        ValueFetcher fetcher = fieldType.valueFetcher("f", null, null);

        SourceLookup missingValueLookup = new SourceLookup();
        SourceLookup nullValueLookup = new SourceLookup();
        nullValueLookup.setSource(Collections.singletonMap("field", null));

        List<Object> ignoredValues = new ArrayList<>();
        assertTrue(fetcher.fetchValues(missingValueLookup, ignoredValues).isEmpty());
        assertTrue(fetcher.fetchValues(nullValueLookup, ignoredValues).isEmpty());

        MappedFieldType valued = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("foo");
        fetcher = valued.valueFetcher("field", null, null);

        assertEquals(List.of("foo"), fetcher.fetchValues(missingValueLookup, ignoredValues));
        assertEquals(List.of("foo"), fetcher.fetchValues(nullValueLookup, ignoredValues));
    }
}
