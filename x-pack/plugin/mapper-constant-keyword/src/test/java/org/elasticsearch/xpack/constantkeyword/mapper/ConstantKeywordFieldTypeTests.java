/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.ConstantFieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper.ConstantKeywordFieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConstantKeywordFieldTypeTests extends ConstantFieldTypeTestCase {

    private SearchExecutionContext mockContext(String field, boolean isVisible) {
        var context = mock(SearchExecutionContext.class);
        when(context.isFieldVisible(field)).thenReturn(isVisible);
        return context;
    }

    public void testTermQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termQuery("foo", mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termQueryCaseInsensitive("fOo", mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("bar", mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQueryCaseInsensitive("bAr", mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQuery("foo", mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termQueryCaseInsensitive("fOo", mockContext("f", false)));
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.termQuery("foo", mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.termQueryCaseInsensitive("fOo", mockContext("f", true)));
    }

    public void testTermsQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.termsQuery(Collections.singletonList("foo"), mockContext("f", true)));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termsQuery(Collections.singletonList("foo"), mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("bar", "foo", "quux"), mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Collections.emptyList(), mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Collections.singletonList("bar"), mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("bar", "quux"), mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Collections.singletonList("foo"), mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.termsQuery(Arrays.asList("bar", "foo", "quux"), mockContext("f", false)));
    }

    public void testWildcardQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.wildcardQuery("f*o", null, false, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.wildcardQuery("F*o", null, true, mockContext("f", true)));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.wildcardQuery("f*o", null, false, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.wildcardQuery("F*o", null, true, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("b*r", null, false, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("B*r", null, true, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("f*o", null, false, mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.wildcardQuery("F*o", null, true, mockContext("f", false)));
    }

    public void testWildcardQueryWithQuestionMark() {
        // gh-141785: ? should match exactly one character, not be treated as a literal.
        ConstantKeywordFieldType foo = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.wildcardQuery("f?o", null, false, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.wildcardQuery("F?o", null, true, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.wildcardQuery("???", null, false, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.wildcardQuery("f?o*", null, false, mockContext("f", true)));
        // ? matches exactly one character, so "f?" against "foo" must not match.
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.wildcardQuery("f?", null, false, mockContext("f", true)));
        // No match against the constant value.
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.wildcardQuery("b?r", null, false, mockContext("f", true)));

        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.wildcardQuery("f?o", null, false, mockContext("f", true)));
    }

    public void testNormalizedWildcardQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.normalizedWildcardQuery("f*", null, mockContext("f", true)));
        ConstantKeywordFieldType foo = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.normalizedWildcardQuery("f*", null, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.normalizedWildcardQuery("*o", null, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.normalizedWildcardQuery("*o*", null, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, foo.normalizedWildcardQuery("f*o", null, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.normalizedWildcardQuery("*ar", null, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.normalizedWildcardQuery("ba*", null, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.normalizedWildcardQuery("f*", null, mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.normalizedWildcardQuery("*o", null, mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.normalizedWildcardQuery("*o*", null, mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, foo.normalizedWildcardQuery("f*o", null, mockContext("f", false)));
    }

    public void testPrefixQuery() {
        ConstantKeywordFieldType bar = new ConstantKeywordFieldType("f", "bar");
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.prefixQuery("fo", null, false, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, bar.prefixQuery("fO", null, true, mockContext("f", true)));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.prefixQuery("fo", null, false, mockContext("f", true)));
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.prefixQuery("fO", null, true, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("ba", null, false, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("Ba", null, true, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("fo", null, false, mockContext("f", false)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.prefixQuery("fO", null, true, mockContext("f", false)));
    }

    public void testExistsQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.existsQuery(mockContext("f", true)));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.existsQuery(mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.existsQuery(mockContext("f", false)));
    }

    public void testRangeQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            none.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            none.rangeQuery(null, "foo", randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            none.rangeQuery("foo", null, randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(
            Queries.ALL_DOCS_INSTANCE,
            ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.ALL_DOCS_INSTANCE,
            ft.rangeQuery("foo", null, true, randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery("foo", null, false, randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.ALL_DOCS_INSTANCE,
            ft.rangeQuery(null, "foo", randomBoolean(), true, null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery(null, "foo", randomBoolean(), false, null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.ALL_DOCS_INSTANCE,
            ft.rangeQuery("abc", "xyz", randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery("abc", "def", randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery("mno", "xyz", randomBoolean(), randomBoolean(), null, null, null, mockContext("f", true))
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, mockContext("f", false))
        );

        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery("foo", null, true, randomBoolean(), null, null, null, mockContext("f", false))
        );

        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery(null, "foo", randomBoolean(), true, null, null, null, mockContext("f", false))
        );

        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.rangeQuery("abc", "xyz", randomBoolean(), randomBoolean(), null, null, null, mockContext("f", false))
        );
    }

    public void testFuzzyQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), mockContext("f", true)));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foobar");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.fuzzyQuery("foobaz", Fuzziness.AUTO, 3, 50, randomBoolean(), mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.fuzzyQuery("fooquux", Fuzziness.AUTO, 3, 50, randomBoolean(), mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.fuzzyQuery("foobaz", Fuzziness.AUTO, 3, 50, randomBoolean(), mockContext("f", false)));
    }

    public void testRegexpQuery() {
        ConstantKeywordFieldType none = new ConstantKeywordFieldType("f", null);
        assertEquals(Queries.NO_DOCS_INSTANCE, none.regexpQuery("f..o", RegExp.ALL, 0, 10, null, mockContext("f", true)));
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        assertEquals(Queries.ALL_DOCS_INSTANCE, ft.regexpQuery("f.o", RegExp.ALL, 0, 10, null, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.regexpQuery("f..o", RegExp.ALL, 0, 10, null, mockContext("f", true)));
        assertEquals(Queries.NO_DOCS_INSTANCE, ft.regexpQuery("f.o", RegExp.ALL, 0, 10, null, mockContext("f", false)));
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
        fetcher = valued.valueFetcher(mockContext("field", true), null);

        assertEquals(List.of("foo"), fetcher.fetchValues(sourceWithNoFieldValue, -1, ignoredValues));
        assertEquals(List.of("foo"), fetcher.fetchValues(sourceWithNullFieldValue, -1, ignoredValues));
    }

    public void testAutomatonQuery() {
        ConstantKeywordFieldType ft = new ConstantKeywordFieldType("f", "foo");
        var automaton = Automata.makeString("foo");

        assertEquals(
            Queries.ALL_DOCS_INSTANCE,
            ft.automatonQuery(() -> automaton, () -> new CharacterRunAutomaton(automaton), null, mockContext("f", true), "test")
        );
        assertEquals(
            Queries.NO_DOCS_INSTANCE,
            ft.automatonQuery(() -> automaton, () -> new CharacterRunAutomaton(automaton), null, mockContext("f", false), "test")
        );
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new ConstantKeywordFieldMapper.ConstantKeywordFieldType(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }
}
