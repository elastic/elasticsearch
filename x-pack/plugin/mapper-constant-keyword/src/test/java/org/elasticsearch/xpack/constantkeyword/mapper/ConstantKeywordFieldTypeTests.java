/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.ConstantFieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper.ConstantKeywordFieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConstantKeywordFieldTypeTests extends ConstantFieldTypeTestCase {

    public void testTermQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termQuery("foo", null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termQueryCaseInsensitive("fOo", null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("bar", null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQueryCaseInsensitive("bAr", null));
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.termQuery("foo", null));
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.termQueryCaseInsensitive("fOo", null));
    }

    public void testTermsQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.termsQuery(Collections.singletonList("foo"), null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termsQuery(Collections.singletonList("foo"), null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("bar", "foo", "quux"), null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Collections.emptyList(), null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Collections.singletonList("bar"), null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("bar", "quux"), null));
    }

    public void testWildcardQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.wildcardQuery("f*o", null, false, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.wildcardQuery("F*o", null, true, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.wildcardQuery("f*o", null, false, null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.wildcardQuery("F*o", null, true, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("b*r", null, false, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("B*r", null, true, null));
    }

    public void testPrefixQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.prefixQuery("fo", null, false, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.prefixQuery("fO", null, true, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.prefixQuery("fo", null, false, null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.prefixQuery("fO", null, true, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("ba", null, false, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("Ba", null, true, null));
    }

    public void testExistsQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.existsQuery(null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.existsQuery(null));
    }

    public void testRangeQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, none.rangeQuery(null, "foo", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, none.rangeQuery("foo", null, randomBoolean(), randomBoolean(), null, null, null, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.rangeQuery("foo", null, true, randomBoolean(), null, null, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.rangeQuery("foo", null, false, randomBoolean(), null, null, null, null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.rangeQuery(null, "foo", randomBoolean(), true, null, null, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.rangeQuery(null, "foo", randomBoolean(), false, null, null, null, null));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.rangeQuery("abc", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.rangeQuery("abc", "def", randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.rangeQuery("mno", "xyz", randomBoolean(), randomBoolean(), null, null, null, null));
    }

    public void testFuzzyQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foobar");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.fuzzyQuery("foobaz", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), null));
    }

    public void testRegexpQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.regexpQuery("f..o", RegExp.ALL, 0, 10, null, null));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.regexpQuery("f.o", RegExp.ALL, 0, 10, null, null));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.regexpQuery("f..o", RegExp.ALL, 0, 10, null, null));
    }

    public void testFetchValue() throws Exception {
        MappedFieldType fieldType = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", null);
        ValueFetcher fetcher = fieldType.valueFetcher(null, null);

        Source sourceWithNoFieldValue = Source.fromMap(Map.of("unrelated", "random"), randomFrom(XContentType.values()));
        Source sourceWithNullFieldValue = Source.fromMap(Collections.singletonMap("field", null), randomFrom(XContentType.values()));

        List<Object> ignoredValues = new ArrayList<>();
        assertTrue(fetcher.fetchValues(sourceWithNoFieldValue, -1, ignoredValues).isEmpty());
        assertTrue(fetcher.fetchValues(sourceWithNullFieldValue, -1, ignoredValues).isEmpty());

        MappedFieldType valued = new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", "foo");
        fetcher = valued.valueFetcher(null, null);

        assertEquals(List.of("foo"), fetcher.fetchValues(sourceWithNoFieldValue, -1, ignoredValues));
        assertEquals(List.of("foo"), fetcher.fetchValues(sourceWithNullFieldValue, -1, ignoredValues));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new ConstantKeywordFieldMapper.ConstantKeywordFieldType(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }
}
