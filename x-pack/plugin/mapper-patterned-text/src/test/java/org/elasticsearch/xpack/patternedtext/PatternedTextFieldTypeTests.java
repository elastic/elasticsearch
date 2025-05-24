/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
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
import org.elasticsearch.index.mapper.extras.SourceIntervalsSource;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

public class PatternedTextFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field", "foo"))), ft.termQuery("foo", null));
        assertEquals(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo")), ft.termQueryCaseInsensitive("fOo", null));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), null));
    }

    public void testRangeQuery() {
        MappedFieldType ft = new PatternedTextFieldType("field");
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
        MappedFieldType ft = new PatternedTextFieldType("field");
        assertEquals(new RegexpQuery(new Term("field", "foo.*")), ft.regexpQuery("foo.*", 0, 0, 10, null, MOCK_CONTEXT));

        ElasticsearchException ee = expectThrows(
            ElasticsearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_CONTEXT_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new PatternedTextFieldType("field");
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

    public void testFetchDocValue() throws IOException {
        Supplier<Document> documentSupplier = () -> {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field.template", new BytesRef("value")));
            return doc;
        };

        MappedFieldType fieldType = new PatternedTextFieldType("field");
        assertEquals(List.of("value"), fetchDocValues(fieldType, documentSupplier));
    }

    private Query unwrapPositionalQuery(Query query) {
        query = ((ConstantScoreQuery) query).getQuery();
        return query;
    }

    public void testPhraseQuery() throws IOException {
        MappedFieldType ft = new PatternedTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 4, 7));
        Query query = ft.phraseQuery(ts, 0, true, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        assertEquals(
            new BooleanQuery.Builder().add(new PhraseQuery("field", "a", "b"), BooleanClause.Occur.SHOULD)
                .add(new PhraseQuery("field.template", "a", "b"), BooleanClause.Occur.SHOULD)
                .build()
                .toString(),
            delegate.toString()
        );
    }

    public void testMultiPhraseQuery() throws IOException {
        MappedFieldType ft = new PatternedTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 0, 0, 3), new Token("c", 4, 7));
        Query query = ft.multiPhraseQuery(ts, 0, true, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        Query expected = new BooleanQuery.Builder().add(
            new MultiPhraseQuery.Builder().add(new Term[] { new Term("field", "a"), new Term("field", "b") })
                .add(new Term("field", "c"))
                .build(),
            BooleanClause.Occur.SHOULD
        )
            .add(
                new MultiPhraseQuery.Builder().add(new Term[] { new Term("field.template", "a"), new Term("field.template", "b") })
                    .add(new Term("field.template", "c"))
                    .build(),
                BooleanClause.Occur.SHOULD
            )
            .build();
        assertEquals(expected.toString(), delegate.toString());
    }

    public void testPhrasePrefixQuery() throws IOException {
        MappedFieldType ft = new PatternedTextFieldType("field");
        TokenStream ts = new CannedTokenStream(new Token("a", 0, 3), new Token("b", 0, 0, 3), new Token("c", 4, 7));
        Query query = ft.phrasePrefixQuery(ts, 0, 10, MOCK_CONTEXT);
        Query delegate = unwrapPositionalQuery(query);
        MultiPhrasePrefixQuery expected = new MultiPhrasePrefixQuery("field");
        expected.add(new Term[] { new Term("field", "a"), new Term("field", "b") });
        expected.add(new Term("field", "c"));
        MultiPhrasePrefixQuery expectedTemplate = new MultiPhrasePrefixQuery("field.template");
        expectedTemplate.add(new Term[] { new Term("field.template", "a"), new Term("field.template", "b") });
        expectedTemplate.add(new Term("field.template", "c"));
        assertEquals(
            new BooleanQuery.Builder().add(expected, BooleanClause.Occur.SHOULD)
                .add(expectedTemplate, BooleanClause.Occur.SHOULD)
                .build()
                .toString(),
            delegate.toString()
        );
    }

    public void testTermIntervals() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        IntervalsSource termIntervals = ft.termIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(termIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(Intervals.term(new BytesRef("foo")), ((SourceIntervalsSource) termIntervals).getIntervalsSource());
    }

    public void testPrefixIntervals() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        IntervalsSource prefixIntervals = ft.prefixIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(prefixIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.prefix(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) prefixIntervals).getIntervalsSource()
        );
    }

    public void testWildcardIntervals() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        IntervalsSource wildcardIntervals = ft.wildcardIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(wildcardIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.wildcard(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) wildcardIntervals).getIntervalsSource()
        );
    }

    public void testRegexpIntervals() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        IntervalsSource regexpIntervals = ft.regexpIntervals(new BytesRef("foo"), MOCK_CONTEXT);
        assertThat(regexpIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.regexp(new BytesRef("foo"), IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) regexpIntervals).getIntervalsSource()
        );
    }

    public void testFuzzyIntervals() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        IntervalsSource fuzzyIntervals = ft.fuzzyIntervals("foo", 1, 2, true, MOCK_CONTEXT);
        assertThat(fuzzyIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
    }

    public void testRangeIntervals() {
        MappedFieldType ft = new PatternedTextFieldType("field");
        IntervalsSource rangeIntervals = ft.rangeIntervals(new BytesRef("foo"), new BytesRef("foo1"), true, true, MOCK_CONTEXT);
        assertThat(rangeIntervals, Matchers.instanceOf(SourceIntervalsSource.class));
        assertEquals(
            Intervals.range(new BytesRef("foo"), new BytesRef("foo1"), true, true, IndexSearcher.getMaxClauseCount()),
            ((SourceIntervalsSource) rangeIntervals).getIntervalsSource()
        );
    }
}
