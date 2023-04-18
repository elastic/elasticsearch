/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.extras.MatchOnlyTextFieldMapper.MatchOnlyTextFieldType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MatchOnlyTextFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field", "foo"))), ft.termQuery("foo", null));
        assertEquals(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo")), ft.termQueryCaseInsensitive("fOo", null));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(
            new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
            ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_CONTEXT)
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(new RegexpQuery(new Term("field", "foo.*")), ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        assertEquals(
            new ConstantScoreQuery(new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true)),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_CONTEXT)
        );

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.fuzzyQuery(
                "foo",
                Fuzziness.AUTO,
                randomInt(10) + 1,
                randomInt(10) + 1,
                randomBoolean(),
                MOCK_CONTEXT_DISALLOW_EXPENSIVE
            )
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFetchSourceValue() throws IOException {
        MatchOnlyTextFieldType fieldType = new MatchOnlyTextFieldType("field");

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));
    }

    private Query unwrapPositionalQuery(Query query) {
        query = ((ConstantScoreQuery) query).getQuery();
        query = ((SourceConfirmedTextQuery) query).getQuery();
        return query;
    }

    public void testPhraseQuery() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));
        Query query = ft.phraseQuery(ts, 0, true, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        assertEquals(new PhraseQuery("field", "a", "b"), delegate);
        assertNotEquals(new MatchAllDocsQuery(), SourceConfirmedTextQuery.approximate(delegate));
    }

    public void testMultiPhraseQuery() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 0, 0, 3), new Token("c", 4, 7));
        Query query = ft.multiPhraseQuery(ts, 0, true, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        MultiPhraseQuery expected = new MultiPhraseQuery.Builder().add(new Term[] { new Term("field", "a"), new Term("field", "b") })
            .add(new Term("field", "c"))
            .build();
        assertEquals(expected, delegate);
        assertNotEquals(new MatchAllDocsQuery(), SourceConfirmedTextQuery.approximate(delegate));
    }

    public void testPhrasePrefixQuery() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 0, 0, 3), new Token("c", 4, 7));
        Query query = ft.phrasePrefixQuery(ts, 0, 10, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        MultiPhrasePrefixQuery expected = new MultiPhrasePrefixQuery("field");
        expected.add(new Term[] { new Term("field", "a"), new Term("field", "b") });
        expected.add(new Term("field", "c"));
        assertEquals(expected, delegate);
        assertNotEquals(new MatchAllDocsQuery(), SourceConfirmedTextQuery.approximate(delegate));
    }

    public void testTermIntervals() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource termIntervals = ft.termIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(termIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(Intervals.term(new BytesRef("foo")), ((SourceIntervalsSource) termIntervals).getIntervalsSource());
    }

    public void testPrefixIntervals() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource prefixIntervals = ft.prefixIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(prefixIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(Intervals.prefix(new BytesRef("foo")), ((SourceIntervalsSource) prefixIntervals).getIntervalsSource());
    }

    public void testWildcardIntervals() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource wildcardIntervals = ft.wildcardIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(wildcardIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(Intervals.wildcard(new BytesRef("foo")), ((SourceIntervalsSource) wildcardIntervals).getIntervalsSource());
    }

    public void testFuzzyIntervals() throws IOException {
        MappedFieldType ft = new MatchOnlyTextFieldType("field");
        IntervalsSource fuzzyIntervals = ft.fuzzyIntervals("foo", 1, 2, true, MOCK_CONTEXT);
        assertThat(fuzzyIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
    }
}
