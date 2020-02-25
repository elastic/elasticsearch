/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.percolator;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.XIntervals;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.percolator.QueryAnalyzer.QueryExtraction;
import org.elasticsearch.percolator.QueryAnalyzer.Result;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.percolator.QueryAnalyzer.analyze;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class QueryAnalyzerTests extends ESTestCase {

    public void testExtractQueryMetadata_termQuery() {
        TermQuery termQuery = new TermQuery(new Term("_field", "_term"));
        Result result = analyze(termQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        assertThat(terms.size(), equalTo(1));
        assertThat(terms.get(0).field(), equalTo(termQuery.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_termsQuery() {
        TermInSetQuery termsQuery = new TermInSetQuery("_field", new BytesRef("_term1"), new BytesRef("_term2"));
        Result result = analyze(termsQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(2));
        assertThat(terms.get(0).field(), equalTo("_field"));
        assertThat(terms.get(0).text(), equalTo("_term1"));
        assertThat(terms.get(1).field(), equalTo("_field"));
        assertThat(terms.get(1).text(), equalTo("_term2"));
    }

    public void testExtractQueryMetadata_phraseQuery() {
        PhraseQuery phraseQuery = new PhraseQuery("_field", "_term1", "term2");
        Result result = analyze(phraseQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(2));
        assertThat(terms.get(0).field(), equalTo(phraseQuery.getTerms()[0].field()));
        assertThat(terms.get(0).bytes(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(terms.get(1).field(), equalTo(phraseQuery.getTerms()[1].field()));
        assertThat(terms.get(1).bytes(), equalTo(phraseQuery.getTerms()[1].bytes()));
    }

    public void testExtractQueryMetadata_multiPhraseQuery() {
        MultiPhraseQuery multiPhraseQuery = new MultiPhraseQuery.Builder()
            .add(new Term("_field", "_term1"))
            .add(new Term[] {new Term("_field", "_term2"), new Term("_field", "_term3")})
            .add(new Term[] {new Term("_field", "_term4"), new Term("_field", "_term5")})
            .add(new Term[] {new Term("_field", "_term6")})
            .build();
        Result result = analyze(multiPhraseQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(4));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(6));
        assertThat(terms.get(0).field(), equalTo("_field"));
        assertThat(terms.get(0).bytes().utf8ToString(), equalTo("_term1"));
        assertThat(terms.get(1).field(), equalTo("_field"));
        assertThat(terms.get(1).bytes().utf8ToString(), equalTo("_term2"));
        assertThat(terms.get(2).field(), equalTo("_field"));
        assertThat(terms.get(2).bytes().utf8ToString(), equalTo("_term3"));
        assertThat(terms.get(3).field(), equalTo("_field"));
        assertThat(terms.get(3).bytes().utf8ToString(), equalTo("_term4"));
        assertThat(terms.get(4).field(), equalTo("_field"));
        assertThat(terms.get(4).bytes().utf8ToString(), equalTo("_term5"));
        assertThat(terms.get(5).field(), equalTo("_field"));
        assertThat(terms.get(5).bytes().utf8ToString(), equalTo("_term6"));
    }

    public void testExtractQueryMetadata_multiPhraseQuery_dups() {
        MultiPhraseQuery multiPhraseQuery = new MultiPhraseQuery.Builder()
            .add(new Term("_field", "_term1"))
            .add(new Term[] {new Term("_field", "_term1"), new Term("_field", "_term2")})
            .build();

        Result result = analyze(multiPhraseQuery, Version.CURRENT);
        assertFalse(result.matchAllDocs);
        assertFalse(result.verified);
        assertTermsEqual(result.extractions, new Term("_field", "_term1"), new Term("_field", "_term2"));
        assertEquals(1, result.minimumShouldMatch); // because of the dup term
    }


    public void testExtractQueryMetadata_booleanQuery() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "term0"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        PhraseQuery phraseQuery = new PhraseQuery("_field", "term1", "term2");
        builder.add(phraseQuery, BooleanClause.Occur.SHOULD);

        BooleanQuery.Builder subBuilder = new BooleanQuery.Builder();
        TermQuery termQuery2 = new TermQuery(new Term("_field1", "term4"));
        subBuilder.add(termQuery2, BooleanClause.Occur.MUST);
        TermQuery termQuery3 = new TermQuery(new Term("_field3", "term5"));
        subBuilder.add(termQuery3, BooleanClause.Occur.MUST);
        builder.add(subBuilder.build(), BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        Result result = analyze(booleanQuery, Version.CURRENT);
        assertThat("Should clause with phrase query isn't verified, so entire query can't be verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(5));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
        assertThat(terms.get(1).field(), equalTo(phraseQuery.getTerms()[0].field()));
        assertThat(terms.get(1).bytes(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(terms.get(2).field(), equalTo(phraseQuery.getTerms()[1].field()));
        assertThat(terms.get(2).bytes(), equalTo(phraseQuery.getTerms()[1].bytes()));
        assertThat(terms.get(3).field(), equalTo(termQuery2.getTerm().field()));
        assertThat(terms.get(3).bytes(), equalTo(termQuery2.getTerm().bytes()));
        assertThat(terms.get(4).field(), equalTo(termQuery3.getTerm().field()));
        assertThat(terms.get(4).bytes(), equalTo(termQuery3.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_booleanQuery_msm() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(2);
        Term term1 = new Term("_field", "_term1");
        TermQuery termQuery1 = new TermQuery(term1);
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        Term term2 = new Term("_field", "_term2");
        TermQuery termQuery2 = new TermQuery(term2);
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        Term term3 = new Term("_field", "_term3");
        TermQuery termQuery3 = new TermQuery(term3);
        builder.add(termQuery3, BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        Result result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, term1, term2, term3);

        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.SHOULD)
                        .add(termQuery2, Occur.SHOULD)
                        .build(), Occur.SHOULD)
                .add(termQuery3, Occur.SHOULD)
                .setMinimumNumberShouldMatch(2);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, term1, term2, term3);

        Term term4 = new Term("_field", "_term4");
        TermQuery termQuery4 = new TermQuery(term4);
        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.MUST)
                        .add(termQuery2, Occur.FILTER)
                        .build(), Occur.SHOULD)
                .add(new BooleanQuery.Builder()
                        .add(termQuery3, Occur.MUST)
                        .add(termQuery4, Occur.FILTER)
                        .build(), Occur.SHOULD);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, term1, term2, term3, term4);

        Term term5 = new Term("_field", "_term5");
        TermQuery termQuery5 = new TermQuery(term5);
        builder.add(termQuery5, Occur.SHOULD);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, term1, term2, term3, term4, term5);

        builder.setMinimumNumberShouldMatch(2);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertTermsEqual(result.extractions, term1, term2, term3, term4, term5);

        builder.setMinimumNumberShouldMatch(3);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(5));
        assertTermsEqual(result.extractions, term1, term2, term3, term4, term5);

        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.SHOULD)
                        .add(termQuery2, Occur.SHOULD)
                        .build(), Occur.SHOULD)
                .add(new BooleanQuery.Builder().setMinimumNumberShouldMatch(1).build(), Occur.SHOULD)
                .setMinimumNumberShouldMatch(2);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        // ideally it would return no extractions, but the fact
        // that it doesn't consider them verified is probably good enough
        assertFalse(result.verified);
    }

    public void testExtractQueryMetadata_booleanQuery_onlyShould() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term1"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("_field", "_term2"));
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);

        BooleanQuery.Builder subBuilder = new BooleanQuery.Builder();
        TermQuery termQuery3 = new TermQuery(new Term("_field1", "_term"));
        subBuilder.add(termQuery3, BooleanClause.Occur.SHOULD);
        TermQuery termQuery4 = new TermQuery(new Term("_field3", "_long_term"));
        subBuilder.add(termQuery4, BooleanClause.Occur.SHOULD);
        builder.add(subBuilder.build(), BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        Result result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryAnalyzer.QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(4));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
        assertThat(terms.get(1).field(), equalTo(termQuery2.getTerm().field()));
        assertThat(terms.get(1).bytes(), equalTo(termQuery2.getTerm().bytes()));
        assertThat(terms.get(2).field(), equalTo(termQuery3.getTerm().field()));
        assertThat(terms.get(2).bytes(), equalTo(termQuery3.getTerm().bytes()));
        assertThat(terms.get(3).field(), equalTo(termQuery4.getTerm().field()));
        assertThat(terms.get(3).bytes(), equalTo(termQuery4.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_booleanQueryWithMustNot() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        builder.add(termQuery1, BooleanClause.Occur.MUST_NOT);
        PhraseQuery phraseQuery = new PhraseQuery("_field", "_term1", "term2");
        builder.add(phraseQuery, BooleanClause.Occur.SHOULD);
        BooleanQuery booleanQuery = builder.build();

        Result result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, phraseQuery.getTerms());

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.MUST_NOT);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        booleanQuery = builder.build();
        result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertTermsEqual(result.extractions);
    }

    public void testExactMatch_booleanQuery() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term1"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("_field", "_term2"));
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        Result result = analyze(builder.build(), Version.CURRENT);
        assertThat("All clauses are exact, so candidate matches are verified", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        PhraseQuery phraseQuery1 = new PhraseQuery("_field", "_term1", "_term2");
        builder.add(phraseQuery1, BooleanClause.Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Clause isn't exact, so candidate matches are not verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder();
        builder.add(phraseQuery1, BooleanClause.Occur.SHOULD);
        PhraseQuery phraseQuery2 = new PhraseQuery("_field", "_term3", "_term4");
        builder.add(phraseQuery2, BooleanClause.Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("No clause is exact, so candidate matches are not verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.MUST_NOT);
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("There is a must_not clause, so candidate matches are not verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder();
        int msm = randomIntBetween(2, 3);
        builder.setMinimumNumberShouldMatch(msm);
        TermQuery termQuery3 = new TermQuery(new Term("_field", "_term3"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        builder.add(termQuery3, BooleanClause.Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Minimum match has no impact on whether the result is verified", result.verified, is(true));
        assertThat("msm is at least two so result.minimumShouldMatch should 2 too", result.minimumShouldMatch, equalTo(msm));

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, randomBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.FILTER);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Also required clauses are taken into account whether the result is verified", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, randomBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.FILTER);
        builder.add(termQuery2, randomBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.FILTER);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Also required clauses are taken into account whether the result is verified", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(2));

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, randomBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.FILTER);
        builder.add(termQuery2, BooleanClause.Occur.MUST_NOT);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Prohibited clause, so candidate matches are not verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, randomBoolean() ? BooleanClause.Occur.MUST : BooleanClause.Occur.FILTER);
        builder.add(termQuery2, BooleanClause.Occur.MUST_NOT);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Prohibited clause, so candidate matches are not verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.FILTER)
                        .add(termQuery2, Occur.FILTER)
                        .build(), Occur.SHOULD)
                .add(termQuery3, Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Inner clause that is not a pure disjunction, so candidate matches are not verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.SHOULD)
                        .add(termQuery2, Occur.SHOULD)
                        .build(), Occur.SHOULD)
                .add(termQuery3, Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Inner clause that is a pure disjunction, so candidate matches are verified", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));

        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.SHOULD)
                        .add(termQuery2, Occur.SHOULD)
                        .build(), Occur.MUST)
                .add(termQuery3, Occur.FILTER);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Disjunctions of conjunctions can't be verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));

        builder = new BooleanQuery.Builder()
                .add(new BooleanQuery.Builder()
                        .add(termQuery1, Occur.MUST)
                        .add(termQuery2, Occur.FILTER)
                        .build(), Occur.SHOULD)
                .add(termQuery3, Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Conjunctions of disjunctions can't be verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
    }

    public void testBooleanQueryWithMustAndShouldClauses() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term1"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("_field", "_term2"));
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        TermQuery termQuery3 = new TermQuery(new Term("_field", "_term3"));
        builder.add(termQuery3, BooleanClause.Occur.MUST);
        Result result = analyze(builder.build(), Version.CURRENT);
        assertThat("Must clause is exact, so this is a verified candidate match", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(result.extractions.size(), equalTo(1));
        List<QueryExtraction> extractions = new ArrayList<>(result.extractions);
        assertThat(extractions.get(0).term, equalTo(new Term("_field", "_term3")));

        builder.setMinimumNumberShouldMatch(1);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Must clause is exact, but m_s_m is 1 so one should clause must match too", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, termQuery1.getTerm(), termQuery2.getTerm(), termQuery3.getTerm());

        builder = new BooleanQuery.Builder();
        BooleanQuery.Builder innerBuilder = new BooleanQuery.Builder();
        innerBuilder.setMinimumNumberShouldMatch(2);
        innerBuilder.add(termQuery1, BooleanClause.Occur.SHOULD);
        innerBuilder.add(termQuery2, BooleanClause.Occur.SHOULD);
        builder.add(innerBuilder.build(), BooleanClause.Occur.MUST);
        builder.add(termQuery3, BooleanClause.Occur.MUST);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Verified, because m_s_m is specified in an inner clause and not top level clause", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertThat(result.extractions.size(), equalTo(3));
        extractions = new ArrayList<>(result.extractions);
        extractions.sort(Comparator.comparing(key -> key.term));
        assertThat(extractions.get(0).term, equalTo(new Term("_field", "_term1")));
        assertThat(extractions.get(1).term, equalTo(new Term("_field", "_term2")));
        assertThat(extractions.get(2).term, equalTo(new Term("_field", "_term3")));

        builder = new BooleanQuery.Builder();
        builder.add(innerBuilder.build(), BooleanClause.Occur.SHOULD);
        builder.add(termQuery3, BooleanClause.Occur.MUST);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Verified, because m_s_m is specified in an inner clause and not top level clause", result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(result.extractions.size(), equalTo(1));
        extractions = new ArrayList<>(result.extractions);
        assertThat(extractions.get(0).term, equalTo(new Term("_field", "_term3")));
    }

    public void testExtractQueryMetadata_constantScoreQuery() {
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        ConstantScoreQuery constantScoreQuery = new ConstantScoreQuery(termQuery1);
        Result result = analyze(constantScoreQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        assertThat(terms.size(), equalTo(1));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_boostQuery() {
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        BoostQuery constantScoreQuery = new BoostQuery(termQuery1, 1f);
        Result result = analyze(constantScoreQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        assertThat(terms.size(), equalTo(1));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_blendedTermQuery() {
        Term[] termsArr = new Term[]{new Term("_field", "_term1"), new Term("_field", "_term2")};
        BlendedTermQuery blendedTermQuery = BlendedTermQuery.dismaxBlendedQuery(termsArr, 1.0f);
        Result result = analyze(blendedTermQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryAnalyzer.QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(2));
        assertThat(terms.get(0).field(), equalTo("_field"));
        assertThat(terms.get(0).text(), equalTo("_term1"));
        assertThat(terms.get(1).field(), equalTo("_field"));
        assertThat(terms.get(1).text(), equalTo("_term2"));
    }

    public void testExtractQueryMetadata_spanTermQuery() {
        // the following span queries aren't exposed in the query dsl and are therefor not supported:
        // 1) SpanPositionRangeQuery
        // 2) PayloadScoreQuery
        // 3) SpanBoostQuery

        // The following span queries can't be supported because of how these queries work:
        // 1) SpanMultiTermQueryWrapper, not supported, because there is no support for MTQ typed queries yet.
        // 2) SpanContainingQuery, is kind of range of spans and we don't know what is between the little and big terms
        // 3) SpanWithinQuery, same reason as SpanContainingQuery
        // 4) FieldMaskingSpanQuery is a tricky query so we shouldn't optimize this

        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        Result result = analyze(spanTermQuery1, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, spanTermQuery1.getTerm());
    }

    public void testExtractQueryMetadata_spanNearQuery() {
        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        SpanTermQuery spanTermQuery2 = new SpanTermQuery(new Term("_field", "_very_long_term"));
        SpanNearQuery spanNearQuery = new SpanNearQuery.Builder("_field", true)
            .addClause(spanTermQuery1).addClause(spanTermQuery2).build();

        Result result = analyze(spanNearQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, spanTermQuery1.getTerm(), spanTermQuery2.getTerm());
    }

    public void testExtractQueryMetadata_spanOrQuery() {
        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        SpanTermQuery spanTermQuery2 = new SpanTermQuery(new Term("_field", "_very_long_term"));
        SpanOrQuery spanOrQuery = new SpanOrQuery(spanTermQuery1, spanTermQuery2);
        Result result = analyze(spanOrQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, spanTermQuery1.getTerm(), spanTermQuery2.getTerm());
    }

    public void testExtractQueryMetadata_spanFirstQuery() {
        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        SpanFirstQuery spanFirstQuery = new SpanFirstQuery(spanTermQuery1, 20);
        Result result = analyze(spanFirstQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, spanTermQuery1.getTerm());
    }

    public void testExtractQueryMetadata_spanNotQuery() {
        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        SpanTermQuery spanTermQuery2 = new SpanTermQuery(new Term("_field", "_very_long_term"));
        SpanNotQuery spanNotQuery = new SpanNotQuery(spanTermQuery1, spanTermQuery2);
        Result result = analyze(spanNotQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, spanTermQuery1.getTerm());
    }

    public void testExtractQueryMetadata_matchNoDocsQuery() {
        Result result = analyze(new MatchNoDocsQuery("sometimes there is no reason at all"), Version.CURRENT);
        assertThat(result.verified, is(true));
        assertEquals(0, result.extractions.size());
        assertThat(result.minimumShouldMatch, equalTo(0));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);
        bq.add(new MatchNoDocsQuery("sometimes there is no reason at all"), BooleanClause.Occur.MUST);
        result = analyze(bq.build(), Version.CURRENT);
        assertThat(result.verified, is(true));
        assertEquals(0, result.extractions.size());
        assertThat(result.minimumShouldMatch, equalTo(0));

        bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.SHOULD);
        bq.add(new MatchNoDocsQuery("sometimes there is no reason at all"), BooleanClause.Occur.SHOULD);
        result = analyze(bq.build(), Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "value"));

        DisjunctionMaxQuery disjunctionMaxQuery = new DisjunctionMaxQuery(
            Arrays.asList(new TermQuery(new Term("field", "value")), new MatchNoDocsQuery("sometimes there is no reason at all")),
            1f
        );
        result = analyze(disjunctionMaxQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "value"));
    }

    public void testExtractQueryMetadata_matchAllDocsQuery() {
        Result result = analyze(new MatchAllDocsQuery(), Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "value"));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        BooleanQuery bq1 = builder.build();
        result = analyze(bq1, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST_NOT);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        BooleanQuery bq2 = builder.build();
        result = analyze(bq2, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        BooleanQuery bq3 = builder.build();
        result = analyze(bq3, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST_NOT);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        BooleanQuery bq4 = builder.build();
        result = analyze(bq4, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));

        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        BooleanQuery bq5 = builder.build();
        result = analyze(bq5, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.SHOULD);
        builder.setMinimumNumberShouldMatch(2);
        BooleanQuery bq6 = builder.build();
        result = analyze(bq6, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(result.extractions.size(), equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "value"));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.SHOULD);
        builder.setMinimumNumberShouldMatch(2);
        BooleanQuery bq7 = builder.build();
        result = analyze(bq7, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));
    }

    public void testExtractQueryMetadata_unsupportedQuery() {
        TermRangeQuery termRangeQuery = new TermRangeQuery("_field", null, null, true, false);
        assertEquals(Result.UNKNOWN, analyze(termRangeQuery, Version.CURRENT));

        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        builder.add(termRangeQuery, BooleanClause.Occur.SHOULD);
        BooleanQuery bq = builder.build();
        assertEquals(Result.UNKNOWN, analyze(bq, Version.CURRENT));
    }

    public void testExtractQueryMetadata_unsupportedQueryInBoolQueryWithMustClauses() {
        TermRangeQuery unsupportedQuery = new TermRangeQuery("_field", null, null, true, false);

        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.MUST);
        builder.add(unsupportedQuery, BooleanClause.Occur.MUST);
        BooleanQuery bq1 = builder.build();

        Result result = analyze(bq1, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, termQuery1.getTerm());

        TermQuery termQuery2 = new TermQuery(new Term("_field", "_longer_term"));
        builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.MUST);
        builder.add(termQuery2, BooleanClause.Occur.MUST);
        builder.add(unsupportedQuery, BooleanClause.Occur.MUST);
        bq1 = builder.build();
        result = analyze(bq1, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, termQuery1.getTerm(), termQuery2.getTerm());

        builder = new BooleanQuery.Builder();
        builder.add(unsupportedQuery, BooleanClause.Occur.MUST);
        builder.add(unsupportedQuery, BooleanClause.Occur.MUST);
        BooleanQuery bq2 = builder.build();
        assertEquals(Result.UNKNOWN, analyze(bq2, Version.CURRENT));
    }

    public void testExtractQueryMetadata_disjunctionMaxQuery() {
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term1"));
        TermQuery termQuery2 = new TermQuery(new Term("_field", "_term2"));
        TermQuery termQuery3 = new TermQuery(new Term("_field", "_term3"));
        TermQuery termQuery4 = new TermQuery(new Term("_field", "_term4"));
        DisjunctionMaxQuery disjunctionMaxQuery = new DisjunctionMaxQuery(
            Arrays.asList(termQuery1, termQuery2, termQuery3, termQuery4), 0.1f
        );

        Result result = analyze(disjunctionMaxQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryAnalyzer.QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(4));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
        assertThat(terms.get(1).field(), equalTo(termQuery2.getTerm().field()));
        assertThat(terms.get(1).bytes(), equalTo(termQuery2.getTerm().bytes()));
        assertThat(terms.get(2).field(), equalTo(termQuery3.getTerm().field()));
        assertThat(terms.get(2).bytes(), equalTo(termQuery3.getTerm().bytes()));
        assertThat(terms.get(3).field(), equalTo(termQuery4.getTerm().field()));
        assertThat(terms.get(3).bytes(), equalTo(termQuery4.getTerm().bytes()));

        disjunctionMaxQuery = new DisjunctionMaxQuery(
            Arrays.asList(termQuery1, termQuery2, termQuery3, new PhraseQuery("_field", "_term4")), 0.1f
        );

        result = analyze(disjunctionMaxQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(4));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
        assertThat(terms.get(1).field(), equalTo(termQuery2.getTerm().field()));
        assertThat(terms.get(1).bytes(), equalTo(termQuery2.getTerm().bytes()));
        assertThat(terms.get(2).field(), equalTo(termQuery3.getTerm().field()));
        assertThat(terms.get(2).bytes(), equalTo(termQuery3.getTerm().bytes()));
        assertThat(terms.get(3).field(), equalTo(termQuery4.getTerm().field()));
        assertThat(terms.get(3).bytes(), equalTo(termQuery4.getTerm().bytes()));
    }

    public void testSynonymQuery() {
        SynonymQuery query = new SynonymQuery();
        Result result = analyze(query, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.isEmpty(), is(true));

        query = new SynonymQuery(new Term("_field", "_value1"), new Term("_field", "_value2"));
        result = analyze(query, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("_field", "_value1"), new Term("_field", "_value2"));
    }

    public void testFunctionScoreQuery() {
        TermQuery termQuery = new TermQuery(new Term("_field", "_value"));
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(termQuery, new RandomScoreFunction(0, 0, null));
        Result result = analyze(functionScoreQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("_field", "_value"));

        functionScoreQuery = new FunctionScoreQuery(termQuery, new RandomScoreFunction(0, 0, null),
            CombineFunction.MULTIPLY, 1f, 10f);
        result = analyze(functionScoreQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("_field", "_value"));
    }

    public void testFunctionScoreQuery_withMatchAll() {
        MatchAllDocsQuery innerQuery = new MatchAllDocsQuery();
        FunctionScoreQuery functionScoreQuery1 = new FunctionScoreQuery(innerQuery, new RandomScoreFunction(0, 0, null));
        Result result = analyze(functionScoreQuery1, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.extractions.isEmpty(), is(true));

        FunctionScoreQuery functionScoreQuery2 =
            new FunctionScoreQuery(innerQuery, new RandomScoreFunction(0, 0, null), CombineFunction.MULTIPLY, 1f, 10f);
        result = analyze(functionScoreQuery2, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.matchAllDocs, is(true));
        assertThat(result.extractions.isEmpty(), is(true));
    }

    public void testPointRangeQuery() {
        // int ranges get converted to long ranges:
        Query query = IntPoint.newRangeQuery("_field", 10, 20);
        Result result = analyze(query, Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryAnalyzer.QueryExtraction> ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertDimension(ranges.get(0).range.lowerPoint, bytes -> IntPoint.encodeDimension(10, bytes, 0));
        assertDimension(ranges.get(0).range.upperPoint, bytes -> IntPoint.encodeDimension(20, bytes, 0));

        query = LongPoint.newRangeQuery("_field", 10L, 21L);
        result = analyze(query, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertFalse(result.verified);
        ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertDimension(ranges.get(0).range.lowerPoint, bytes -> LongPoint.encodeDimension(10L, bytes, 0));
        assertDimension(ranges.get(0).range.upperPoint, bytes -> LongPoint.encodeDimension(21L, bytes, 0));

        // Half float ranges get converted to double ranges:
        query = HalfFloatPoint.newRangeQuery("_field", 10F, 20F);
        result = analyze(query, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertFalse(result.verified);
        ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertDimension(ranges.get(0).range.lowerPoint, bytes -> HalfFloatPoint.encodeDimension(10F, bytes, 0));
        assertDimension(ranges.get(0).range.upperPoint, bytes -> HalfFloatPoint.encodeDimension(20F, bytes, 0));

        // Float ranges get converted to double ranges:
        query = FloatPoint.newRangeQuery("_field", 10F, 20F);
        result = analyze(query, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertFalse(result.verified);
        ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertDimension(ranges.get(0).range.lowerPoint, bytes -> FloatPoint.encodeDimension(10F, bytes, 0));
        assertDimension(ranges.get(0).range.upperPoint, bytes -> FloatPoint.encodeDimension(20F, bytes, 0));

        query = DoublePoint.newRangeQuery("_field", 10D, 20D);
        result = analyze(query, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertFalse(result.verified);
        ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertDimension(ranges.get(0).range.lowerPoint, bytes -> DoublePoint.encodeDimension(10D, bytes, 0));
        assertDimension(ranges.get(0).range.upperPoint, bytes -> DoublePoint.encodeDimension(20D, bytes, 0));

        query = InetAddressPoint.newRangeQuery("_field", InetAddresses.forString("192.168.1.0"),
            InetAddresses.forString("192.168.1.255"));
        result = analyze(query, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertFalse(result.verified);
        ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertArrayEquals(ranges.get(0).range.lowerPoint, InetAddressPoint.encode(InetAddresses.forString("192.168.1.0")));
        assertArrayEquals(ranges.get(0).range.upperPoint, InetAddressPoint.encode(InetAddresses.forString("192.168.1.255")));
    }

    public void testTooManyPointDimensions() {
        // For now no extraction support for geo queries:
        Query query1 = LatLonPoint.newBoxQuery("_field", 0, 1, 0, 1);
        assertEquals(Result.UNKNOWN, analyze(query1, Version.CURRENT));

        Query query2 = LongPoint.newRangeQuery("_field", new long[]{0, 0, 0}, new long[]{1, 1, 1});
        assertEquals(Result.UNKNOWN, analyze(query2, Version.CURRENT));
    }

    public void testPointRangeQuery_lowerUpperReversed() {
        Query query = IntPoint.newRangeQuery("_field", 20, 10);
        Result result = analyze(query, Version.CURRENT);
        assertTrue(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(0));
        assertThat(result.extractions.size(), equalTo(0));
    }

    public void testIndexOrDocValuesQuery() {
        Query query = new IndexOrDocValuesQuery(IntPoint.newRangeQuery("_field", 10, 20),
            SortedNumericDocValuesField.newSlowRangeQuery("_field", 10, 20));
        Result result = analyze(query, Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryAnalyzer.QueryExtraction> ranges = new ArrayList<>(result.extractions);
        assertThat(ranges.size(), equalTo(1));
        assertNull(ranges.get(0).term);
        assertEquals("_field", ranges.get(0).range.fieldName);
        assertDimension(ranges.get(0).range.lowerPoint, bytes -> IntPoint.encodeDimension(10, bytes, 0));
        assertDimension(ranges.get(0).range.upperPoint, bytes -> IntPoint.encodeDimension(20, bytes, 0));
    }

    public void testToParentBlockJoinQuery() {
        TermQuery termQuery = new TermQuery(new Term("field", "value"));
        QueryBitSetProducer queryBitSetProducer = new QueryBitSetProducer(new TermQuery(new Term("_type", "child")));
        ESToParentBlockJoinQuery query = new ESToParentBlockJoinQuery(termQuery, queryBitSetProducer, ScoreMode.None, "child");
        Result result = analyze(query, Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(1, result.extractions.size());
        assertNull(result.extractions.toArray(new QueryExtraction[0])[0].range);
        assertEquals(new Term("field", "value"), result.extractions.toArray(new QueryExtraction[0])[0].term);
    }

    public void testPointRangeQuerySelectRanges() {
        BooleanQuery.Builder boolQuery = new BooleanQuery.Builder();
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.SHOULD);
        boolQuery.add(LongPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.SHOULD);
        Result result = analyze(boolQuery.build(), Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(2, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);
        assertEquals("_field1", new ArrayList<>(result.extractions).get(1).range.fieldName);

        boolQuery = new BooleanQuery.Builder();
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(LongPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.FILTER);
        result = analyze(boolQuery.build(), Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertEquals(2, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);
        assertEquals("_field1", new ArrayList<>(result.extractions).get(1).range.fieldName);

        boolQuery = new BooleanQuery.Builder();
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 15), BooleanClause.Occur.FILTER);
        result = analyze(boolQuery.build(), Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(2, result.extractions.size());
        assertEquals("_field1", new ArrayList<>(result.extractions).get(0).range.fieldName);
        assertEquals("_field1", new ArrayList<>(result.extractions).get(1).range.fieldName);

        boolQuery = new BooleanQuery.Builder().setMinimumNumberShouldMatch(2);
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.SHOULD);
        boolQuery.add(LongPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.SHOULD);
        result = analyze(boolQuery.build(), Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(2, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);
        assertEquals("_field1", new ArrayList<>(result.extractions).get(1).range.fieldName);

        boolQuery = new BooleanQuery.Builder().setMinimumNumberShouldMatch(2);
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.SHOULD);
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 15), BooleanClause.Occur.SHOULD);
        result = analyze(boolQuery.build(), Version.CURRENT);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(2, result.extractions.size());
        assertEquals("_field1", new ArrayList<>(result.extractions).get(0).range.fieldName);
        assertEquals("_field1", new ArrayList<>(result.extractions).get(1).range.fieldName);
    }

    public void testExtractQueryMetadata_duplicatedClauses() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(
                new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.MUST)
                        .build(),
                BooleanClause.Occur.MUST
        );
        builder.add(
                new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("field", "value3")), BooleanClause.Occur.MUST)
                        .build(),
                BooleanClause.Occur.MUST
        );
        builder.add(
                new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("field", "value3")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("field", "value4")), BooleanClause.Occur.MUST)
                        .build(),
                BooleanClause.Occur.MUST
        );
        Result result = analyze(builder.build(), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(4));
        assertTermsEqual(result.extractions, new Term("field", "value1"), new Term("field", "value2"),
                new Term("field", "value3"), new Term("field", "value4"));

        builder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(2);
        builder.add(
                new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.MUST)
                        .build(),
                BooleanClause.Occur.SHOULD
        );
        builder.add(
                new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("field", "value3")), BooleanClause.Occur.MUST)
                        .build(),
                BooleanClause.Occur.SHOULD
        );
        builder.add(
                new BooleanQuery.Builder()
                        .add(new TermQuery(new Term("field", "value3")), BooleanClause.Occur.MUST)
                        .add(new TermQuery(new Term("field", "value4")), BooleanClause.Occur.MUST)
                        .build(),
                BooleanClause.Occur.SHOULD
        );
        result = analyze(builder.build(), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, new Term("field", "value1"), new Term("field", "value2"),
                new Term("field", "value3"), new Term("field", "value4"));
    }

    public void testEmptyQueries() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Result result = analyze(builder.build(), Version.CURRENT);
        assertEquals(result, Result.MATCH_NONE);

        result = analyze(new DisjunctionMaxQuery(Collections.emptyList(), 0f), Version.CURRENT);
        assertEquals(result, Result.MATCH_NONE);
    }

    private static void assertDimension(byte[] expected, Consumer<byte[]> consumer) {
        byte[] dest = new byte[expected.length];
        consumer.accept(dest);
        assertArrayEquals(expected, dest);
    }

    private static void assertTermsEqual(Set<QueryExtraction> actual, Term... expected) {
        assertEquals(Arrays.stream(expected).map(QueryExtraction::new).collect(Collectors.toSet()), actual);
    }

    public void testIntervalQueries() {
        IntervalsSource source = Intervals.or(Intervals.term("term1"), Intervals.term("term2"));
        Result result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "term1"), new Term("field", "term2"));

        source = Intervals.ordered(Intervals.term("term1"), Intervals.term("term2"),
            Intervals.or(Intervals.term("term3"), Intervals.term("term4")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertTermsEqual(result.extractions, new Term("field", "term1"), new Term("field", "term2"),
            new Term("field", "term3"), new Term("field", "term4"));

        source = Intervals.ordered(Intervals.term("term1"), XIntervals.wildcard(new BytesRef("a*")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "term1"));

        source = Intervals.ordered(XIntervals.wildcard(new BytesRef("a*")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertEquals(Result.UNKNOWN, result);

        source = Intervals.or(Intervals.term("b"), XIntervals.wildcard(new BytesRef("a*")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertEquals(Result.UNKNOWN, result);

        source = Intervals.ordered(Intervals.term("term1"), XIntervals.prefix(new BytesRef("a")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "term1"));

        source = Intervals.ordered(XIntervals.prefix(new BytesRef("a")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertEquals(Result.UNKNOWN, result);

        source = Intervals.or(Intervals.term("b"), XIntervals.prefix(new BytesRef("a")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertEquals(Result.UNKNOWN, result);

        source = Intervals.containedBy(Intervals.term("a"), Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertTermsEqual(result.extractions, new Term("field", "a"), new Term("field", "b"), new Term("field", "c"));

        source = Intervals.containing(Intervals.term("a"), Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertTermsEqual(result.extractions, new Term("field", "a"), new Term("field", "b"), new Term("field", "c"));

        source = Intervals.overlapping(Intervals.term("a"), Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertTermsEqual(result.extractions, new Term("field", "a"), new Term("field", "b"), new Term("field", "c"));

        source = Intervals.within(Intervals.term("a"), 2, Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertTermsEqual(result.extractions, new Term("field", "a"), new Term("field", "b"), new Term("field", "c"));

        source = Intervals.notContainedBy(Intervals.term("a"), Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "a"));

        source = Intervals.notContaining(Intervals.term("a"), Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "a"));

        source = Intervals.nonOverlapping(Intervals.term("a"), Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "a"));

        source = Intervals.notWithin(Intervals.term("a"), 2, Intervals.ordered(Intervals.term("b"), Intervals.term("c")));
        result = analyze(new IntervalQuery("field", source), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.matchAllDocs, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "a"));
    }

    public void testRangeAndTermWithNestedMSM() {

        Query q1 = new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f", "v3")), Occur.SHOULD)
            .add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "n1")), Occur.SHOULD)
                .build(), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v4")), Occur.SHOULD)
            .setMinimumNumberShouldMatch(2)
            .build();

        Result r1 = analyze(q1, Version.CURRENT);
        assertEquals(2, r1.minimumShouldMatch);
        assertThat(r1.extractions, hasSize(3));
        assertFalse(r1.matchAllDocs);
        assertTrue(r1.verified);

        Query q = new BooleanQuery.Builder()
            .add(IntPoint.newRangeQuery("i", 0, 10), Occur.FILTER)
            .add(new TermQuery(new Term("f", "v1")), Occur.MUST)
            .add(new TermQuery(new Term("f", "v2")), Occur.MUST)
            .add(IntPoint.newRangeQuery("i", 2, 20), Occur.FILTER)
            .add(new TermQuery(new Term("f", "v3")), Occur.SHOULD)
            .add(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "n1")), Occur.SHOULD)
                .build(), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v4")), Occur.SHOULD)
            .setMinimumNumberShouldMatch(2)
            .build();

        Result r = analyze(q, Version.CURRENT);
        assertThat(r.minimumShouldMatch, equalTo(5));
        assertThat(r.extractions, hasSize(7));
        assertFalse(r.matchAllDocs);
        assertFalse(r.verified);
    }

    public void testCombinedRangeAndTermWithMinimumShouldMatch() {

        Query disj = new BooleanQuery.Builder()
            .add(IntPoint.newRangeQuery("i", 0, 10), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.SHOULD)
            .setMinimumNumberShouldMatch(2)
            .build();

        Result r = analyze(disj, Version.CURRENT);
        assertThat(r.minimumShouldMatch, equalTo(1));
        assertThat(r.extractions, hasSize(2));
        assertFalse(r.matchAllDocs);
        assertFalse(r.verified);

        Query q = new BooleanQuery.Builder()
            .add(IntPoint.newRangeQuery("i", 0, 10), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.FILTER)
            .setMinimumNumberShouldMatch(2)
            .build();

        Result result = analyze(q, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(result.extractions.size(), equalTo(2));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        q = new BooleanQuery.Builder()
            .add(q, Occur.MUST)
            .add(q, Occur.MUST)
            .build();

        result = analyze(q, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(result.extractions.size(), equalTo(2));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        Query q2 = new BooleanQuery.Builder()
            .add(new TermQuery(new Term("f", "v1")), Occur.FILTER)
            .add(IntPoint.newRangeQuery("i", 15, 20), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v2")), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v2")), Occur.MUST)
            .setMinimumNumberShouldMatch(1)
            .build();

        result = analyze(q2, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertThat(result.extractions, hasSize(3));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        // multiple range queries on different fields
        Query q3 = new BooleanQuery.Builder()
            .add(IntPoint.newRangeQuery("i", 15, 20), Occur.SHOULD)
            .add(IntPoint.newRangeQuery("i2", 15, 20), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v2")), Occur.MUST)
            .setMinimumNumberShouldMatch(1)
            .build();
        result = analyze(q3, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertThat(result.extractions, hasSize(4));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        // multiple disjoint range queries on the same field
        Query q4 = new BooleanQuery.Builder()
            .add(IntPoint.newRangeQuery("i", 15, 20), Occur.SHOULD)
            .add(IntPoint.newRangeQuery("i", 25, 30), Occur.SHOULD)
            .add(IntPoint.newRangeQuery("i", 35, 40), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v1")), Occur.SHOULD)
            .add(new TermQuery(new Term("f", "v2")), Occur.MUST)
            .setMinimumNumberShouldMatch(1)
            .build();
        result = analyze(q4, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertThat(result.extractions, hasSize(5));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        // multiple conjunction range queries on the same field
        Query q5 = new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(IntPoint.newRangeQuery("i", 15, 20), Occur.MUST)
                .add(IntPoint.newRangeQuery("i", 25, 30), Occur.MUST)
                .build(), Occur.MUST)
            .add(IntPoint.newRangeQuery("i", 35, 40), Occur.MUST)
            .add(new TermQuery(new Term("f", "v2")), Occur.MUST)
            .build();
        result = analyze(q5, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertThat(result.extractions, hasSize(4));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        // multiple conjunction range queries on different fields
        Query q6 = new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(IntPoint.newRangeQuery("i", 15, 20), Occur.MUST)
                .add(IntPoint.newRangeQuery("i2", 25, 30), Occur.MUST)
                .build(), Occur.MUST)
            .add(IntPoint.newRangeQuery("i", 35, 40), Occur.MUST)
            .add(new TermQuery(new Term("f", "v2")), Occur.MUST)
            .build();
        result = analyze(q6, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertThat(result.extractions, hasSize(4));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);

        // mixed term and range conjunctions
        Query q7 = new BooleanQuery.Builder()
            .add(new BooleanQuery.Builder()
                .add(IntPoint.newRangeQuery("i", 1, 2), Occur.MUST)
                .add(new TermQuery(new Term("f", "1")), Occur.MUST)
                .build(), Occur.MUST)
            .add(new BooleanQuery.Builder()
                .add(IntPoint.newRangeQuery("i", 1, 2), Occur.MUST)
                .add(new TermQuery(new Term("f", "2")), Occur.MUST)
                .build(), Occur.MUST)
            .build();
        result = analyze(q7, Version.CURRENT);
        assertThat(result.minimumShouldMatch, equalTo(3));
        assertThat(result.extractions, hasSize(3));
        assertFalse(result.verified);
        assertFalse(result.matchAllDocs);
    }

}
