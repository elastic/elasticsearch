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
import org.apache.lucene.queries.CommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.percolator.QueryAnalyzer.UnsupportedQueryException;
import static org.elasticsearch.percolator.QueryAnalyzer.analyze;
import static org.elasticsearch.percolator.QueryAnalyzer.selectBestExtraction;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testExtractQueryMetadata_multiPhraseQuery_pre6dot1() {
        MultiPhraseQuery multiPhraseQuery = new MultiPhraseQuery.Builder()
            .add(new Term("_field", "_long_term"))
            .add(new Term[] {new Term("_field", "_long_term"), new Term("_field", "_term")})
            .add(new Term[] {new Term("_field", "_long_term"), new Term("_field", "_very_long_term")})
            .add(new Term[] {new Term("_field", "_very_long_term")})
            .build();
        Result result = analyze(multiPhraseQuery, Version.V_6_0_0);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        assertThat(terms.size(), equalTo(1));
        assertThat(terms.get(0).field(), equalTo("_field"));
        assertThat(terms.get(0).bytes().utf8ToString(), equalTo("_very_long_term"));
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

    public void testExtractQueryMetadata_booleanQuery_pre6dot1() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        PhraseQuery phraseQuery = new PhraseQuery("_field", "_term1", "term2");
        builder.add(phraseQuery, BooleanClause.Occur.SHOULD);

        BooleanQuery.Builder subBuilder = new BooleanQuery.Builder();
        TermQuery termQuery2 = new TermQuery(new Term("_field1", "_term"));
        subBuilder.add(termQuery2, BooleanClause.Occur.MUST);
        TermQuery termQuery3 = new TermQuery(new Term("_field3", "_long_term"));
        subBuilder.add(termQuery3, BooleanClause.Occur.MUST);
        builder.add(subBuilder.build(), BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        Result result = analyze(booleanQuery, Version.V_6_0_0);
        assertThat("Should clause with phrase query isn't verified, so entire query can't be verified", result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(3));
        assertThat(terms.get(0).field(), equalTo(termQuery1.getTerm().field()));
        assertThat(terms.get(0).bytes(), equalTo(termQuery1.getTerm().bytes()));
        assertThat(terms.get(1).field(), equalTo(phraseQuery.getTerms()[0].field()));
        assertThat(terms.get(1).bytes(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(terms.get(2).field(), equalTo(termQuery3.getTerm().field()));
        assertThat(terms.get(2).bytes(), equalTo(termQuery3.getTerm().bytes()));
    }

    public void testExtractQueryMetadata_booleanQuery_msm() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(2);
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term1"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("_field", "_term2"));
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        TermQuery termQuery3 = new TermQuery(new Term("_field", "_term3"));
        builder.add(termQuery3, BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        Result result = analyze(booleanQuery, Version.CURRENT);
        assertThat(result.verified, is(true));
        assertThat(result.minimumShouldMatch, equalTo(2));
        List<QueryExtraction> extractions = new ArrayList<>(result.extractions);
        extractions.sort(Comparator.comparing(extraction -> extraction.term));
        assertThat(extractions.size(), equalTo(3));
        assertThat(extractions.get(0).term, equalTo(new Term("_field", "_term1")));
        assertThat(extractions.get(1).term, equalTo(new Term("_field", "_term2")));
        assertThat(extractions.get(2).term, equalTo(new Term("_field", "_term3")));
    }

    public void testExtractQueryMetadata_booleanQuery_msm_pre6dot1() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.setMinimumNumberShouldMatch(2);
        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term1"));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        TermQuery termQuery2 = new TermQuery(new Term("_field", "_term2"));
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        TermQuery termQuery3 = new TermQuery(new Term("_field", "_term3"));
        builder.add(termQuery3, BooleanClause.Occur.SHOULD);

        BooleanQuery booleanQuery = builder.build();
        Result result = analyze(booleanQuery, Version.V_6_0_0);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> extractions = new ArrayList<>(result.extractions);
        extractions.sort(Comparator.comparing(extraction -> extraction.term));
        assertThat(extractions.size(), equalTo(3));
        assertThat(extractions.get(0).term, equalTo(new Term("_field", "_term1")));
        assertThat(extractions.get(1).term, equalTo(new Term("_field", "_term2")));
        assertThat(extractions.get(2).term, equalTo(new Term("_field", "_term3")));
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
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        assertThat(terms.size(), equalTo(2));
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.get(0).field(), equalTo(phraseQuery.getTerms()[0].field()));
        assertThat(terms.get(0).bytes(), equalTo(phraseQuery.getTerms()[0].bytes()));
        assertThat(terms.get(1).field(), equalTo(phraseQuery.getTerms()[1].field()));
        assertThat(terms.get(1).bytes(), equalTo(phraseQuery.getTerms()[1].bytes()));
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
        builder.setMinimumNumberShouldMatch(randomIntBetween(2, 32));
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        builder.add(termQuery2, BooleanClause.Occur.SHOULD);
        result = analyze(builder.build(), Version.CURRENT);
        assertThat("Minimum match has not impact on whether the result is verified", result.verified, is(true));
        assertThat("msm is at least two so result.minimumShouldMatch should 2 too", result.minimumShouldMatch, equalTo(2));

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
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(result.extractions.size(), equalTo(1));
        extractions = new ArrayList<>(result.extractions);
        assertThat(extractions.get(0).term, equalTo(new Term("_field", "_term3")));

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

    public void testExtractQueryMetadata_commonTermsQuery() {
        CommonTermsQuery commonTermsQuery = new CommonTermsQuery(BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, 100);
        commonTermsQuery.add(new Term("_field", "_term1"));
        commonTermsQuery.add(new Term("_field", "_term2"));
        Result result = analyze(commonTermsQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        List<QueryExtraction> terms = new ArrayList<>(result.extractions);
        terms.sort(Comparator.comparing(qt -> qt.term));
        assertThat(terms.size(), equalTo(2));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertThat(terms.get(0).field(), equalTo("_field"));
        assertThat(terms.get(0).text(), equalTo("_term1"));
        assertThat(terms.get(1).field(), equalTo("_field"));
        assertThat(terms.get(1).text(), equalTo("_term2"));
    }

    public void testExtractQueryMetadata_blendedTermQuery() {
        Term[] termsArr = new Term[]{new Term("_field", "_term1"), new Term("_field", "_term2")};
        BlendedTermQuery commonTermsQuery = BlendedTermQuery.dismaxBlendedQuery(termsArr, 1.0f);
        Result result = analyze(commonTermsQuery, Version.CURRENT);
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

    public void testExtractQueryMetadata_spanNearQuery_pre6dot1() {
        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        SpanTermQuery spanTermQuery2 = new SpanTermQuery(new Term("_field", "_very_long_term"));
        SpanNearQuery spanNearQuery = new SpanNearQuery.Builder("_field", true)
            .addClause(spanTermQuery1).addClause(spanTermQuery2).build();

        Result result = analyze(spanNearQuery, Version.V_6_0_0);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, spanTermQuery2.getTerm());
    }

    public void testExtractQueryMetadata_spanOrQuery() {
        SpanTermQuery spanTermQuery1 = new SpanTermQuery(new Term("_field", "_short_term"));
        SpanTermQuery spanTermQuery2 = new SpanTermQuery(new Term("_field", "_very_long_term"));
        SpanOrQuery spanOrQuery = new SpanOrQuery(spanTermQuery1, spanTermQuery2);
        Result result = analyze(spanOrQuery, Version.CURRENT);
        assertThat(result.verified, is(false));
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
        assertThat(result.minimumShouldMatch, equalTo(1));

        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        bq.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);
        bq.add(new MatchNoDocsQuery("sometimes there is no reason at all"), BooleanClause.Occur.MUST);
        result = analyze(bq.build(), Version.CURRENT);
        assertThat(result.verified, is(true));
        assertEquals(1, result.extractions.size());
        assertThat(result.minimumShouldMatch, equalTo(2));
        assertTermsEqual(result.extractions, new Term("field", "value"));

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
        expectThrows(UnsupportedQueryException.class, () -> analyze(new MatchAllDocsQuery(), Version.CURRENT));

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        Result result = analyze(builder.build(), Version.CURRENT);
        assertThat(result.verified, is(false));
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertTermsEqual(result.extractions, new Term("field", "value"));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        BooleanQuery bq1 = builder.build();
        expectThrows(UnsupportedQueryException.class, () -> analyze(bq1, Version.CURRENT));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST_NOT);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
        BooleanQuery bq2 = builder.build();
        expectThrows(UnsupportedQueryException.class, () -> analyze(bq2, Version.CURRENT));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        BooleanQuery bq3 = builder.build();
        expectThrows(UnsupportedQueryException.class, () -> analyze(bq3, Version.CURRENT));

        builder = new BooleanQuery.Builder();
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST_NOT);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        BooleanQuery bq4 = builder.build();
        expectThrows(UnsupportedQueryException.class, () -> analyze(bq4, Version.CURRENT));

        builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("field", "value")), BooleanClause.Occur.SHOULD);
        builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
        BooleanQuery bq5 = builder.build();
        expectThrows(UnsupportedQueryException.class, () -> analyze(bq5, Version.CURRENT));
    }

    public void testExtractQueryMetadata_unsupportedQuery() {
        TermRangeQuery termRangeQuery = new TermRangeQuery("_field", null, null, true, false);
        UnsupportedQueryException e = expectThrows(UnsupportedQueryException.class,
            () -> analyze(termRangeQuery, Version.CURRENT));
        assertThat(e.getUnsupportedQuery(), sameInstance(termRangeQuery));

        TermQuery termQuery1 = new TermQuery(new Term("_field", "_term"));
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(termQuery1, BooleanClause.Occur.SHOULD);
        builder.add(termRangeQuery, BooleanClause.Occur.SHOULD);
        BooleanQuery bq = builder.build();

        e = expectThrows(UnsupportedQueryException.class, () -> analyze(bq, Version.CURRENT));
        assertThat(e.getUnsupportedQuery(), sameInstance(termRangeQuery));
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
        UnsupportedQueryException e = expectThrows(UnsupportedQueryException.class, () -> analyze(bq2, Version.CURRENT));
        assertThat(e.getUnsupportedQuery(), sameInstance(unsupportedQuery));
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
        assertThat(result.minimumShouldMatch, equalTo(1));
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

    public void testSelectBestExtraction() {
        Set<QueryExtraction> queryTerms1 = terms(new int[0], "12", "1234", "12345");
        Set<QueryAnalyzer.QueryExtraction> queryTerms2 = terms(new int[0], "123", "1234", "12345");
        Set<QueryExtraction> result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms2, result);

        queryTerms1 = terms(new int[]{1, 2, 3});
        queryTerms2 = terms(new int[]{2, 3, 4});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms1, result);

        queryTerms1 = terms(new int[]{4, 5, 6});
        queryTerms2 = terms(new int[]{1, 2, 3});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms2, result);

        queryTerms1 = terms(new int[]{1, 2, 3}, "123", "456");
        queryTerms2 = terms(new int[]{2, 3, 4}, "123", "456");
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms1, result);

        queryTerms1 = terms(new int[]{10});
        queryTerms2 = terms(new int[]{1});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms2, result);

        queryTerms1 = terms(new int[]{10}, "123");
        queryTerms2 = terms(new int[]{1});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms1, result);

        queryTerms1 = terms(new int[]{10}, "1", "123");
        queryTerms2 = terms(new int[]{1}, "1", "2");
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame(queryTerms1, result);

        queryTerms1 = terms(new int[]{1, 2, 3}, "123", "456");
        queryTerms2 = terms(new int[]{2, 3, 4}, "1", "456");
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame("Ignoring ranges, so then prefer queryTerms1, because it has the longest shortest term", queryTerms1, result);

        queryTerms1 = terms(new int[]{});
        queryTerms2 = terms(new int[]{});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame("In case query extractions are empty", queryTerms2, result);

        queryTerms1 = terms(new int[]{1});
        queryTerms2 = terms(new int[]{});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame("In case query a single extraction is empty", queryTerms1, result);

        queryTerms1 = terms(new int[]{});
        queryTerms2 = terms(new int[]{1});
        result = selectBestExtraction(queryTerms1, queryTerms2);
        assertSame("In case query a single extraction is empty", queryTerms2, result);
    }

    public void testSelectBestExtraction_random() {
        Set<QueryExtraction> terms1 = new HashSet<>();
        int shortestTerms1Length = Integer.MAX_VALUE;
        int sumTermLength = randomIntBetween(1, 128);
        while (sumTermLength > 0) {
            int length = randomInt(sumTermLength);
            shortestTerms1Length = Math.min(shortestTerms1Length, length);
            terms1.add(new QueryExtraction(new Term("field", randomAlphaOfLength(length))));
            sumTermLength -= length;
        }

        Set<QueryExtraction> terms2 = new HashSet<>();
        int shortestTerms2Length = Integer.MAX_VALUE;
        sumTermLength = randomIntBetween(1, 128);
        while (sumTermLength > 0) {
            int length = randomInt(sumTermLength);
            shortestTerms2Length = Math.min(shortestTerms2Length, length);
            terms2.add(new QueryExtraction(new Term("field", randomAlphaOfLength(length))));
            sumTermLength -= length;
        }

        Set<QueryAnalyzer.QueryExtraction> result = selectBestExtraction(terms1, terms2);
        Set<QueryExtraction> expected = shortestTerms1Length >= shortestTerms2Length ? terms1 : terms2;
        assertThat(result, sameInstance(expected));
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
        expectThrows(UnsupportedQueryException.class, () -> analyze(query1, Version.CURRENT));

        Query query2 = LongPoint.newRangeQuery("_field", new long[]{0, 0, 0}, new long[]{1, 1, 1});
        expectThrows(UnsupportedQueryException.class, () -> analyze(query2, Version.CURRENT));
    }

    public void testPointRangeQuery_lowerUpperReversed() {
        Query query = IntPoint.newRangeQuery("_field", 20, 10);
        Result result = analyze(query, Version.CURRENT);
        assertTrue(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
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

    public void testPointRangeQuerySelectShortestRange() {
        BooleanQuery.Builder boolQuery = new BooleanQuery.Builder();
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(LongPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.FILTER);
        Result result = analyze(boolQuery.build(), Version.V_6_0_0);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(1, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);

        boolQuery = new BooleanQuery.Builder();
        boolQuery.add(LongPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(IntPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.FILTER);
        result = analyze(boolQuery.build(), Version.V_6_0_0);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(1, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);

        boolQuery = new BooleanQuery.Builder();
        boolQuery.add(DoublePoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(DoublePoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.FILTER);
        result = analyze(boolQuery.build(), Version.V_6_0_0);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(1, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);

        boolQuery = new BooleanQuery.Builder();
        boolQuery.add(DoublePoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(FloatPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.FILTER);
        result = analyze(boolQuery.build(), Version.V_6_0_0);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(1, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);

        boolQuery = new BooleanQuery.Builder();
        boolQuery.add(HalfFloatPoint.newRangeQuery("_field1", 10, 20), BooleanClause.Occur.FILTER);
        boolQuery.add(HalfFloatPoint.newRangeQuery("_field2", 10, 15), BooleanClause.Occur.FILTER);
        result = analyze(boolQuery.build(), Version.V_6_0_0);
        assertFalse(result.verified);
        assertThat(result.minimumShouldMatch, equalTo(1));
        assertEquals(1, result.extractions.size());
        assertEquals("_field2", new ArrayList<>(result.extractions).get(0).range.fieldName);
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
        assertThat(result.minimumShouldMatch, equalTo(2));
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

    private static void assertDimension(byte[] expected, Consumer<byte[]> consumer) {
        byte[] dest = new byte[expected.length];
        consumer.accept(dest);
        assertArrayEquals(expected, dest);
    }

    private static void assertTermsEqual(Set<QueryExtraction> actual, Term... expected) {
        assertEquals(Arrays.stream(expected).map(QueryExtraction::new).collect(Collectors.toSet()), actual);
    }

    private static Set<QueryExtraction> terms(int[] intervals, String... values) {
        Set<QueryExtraction> queryExtractions = new HashSet<>();
        for (int interval : intervals) {
            byte[] encodedInterval = new byte[4];
            IntPoint.encodeDimension(interval, encodedInterval, 0);
            queryExtractions.add(new QueryAnalyzer.QueryExtraction(new QueryAnalyzer.Range("_field", null, null, encodedInterval)));
        }
        for (String value : values) {
            queryExtractions.add(new QueryExtraction(new Term("_field", value)));
        }
        return queryExtractions;
    }

}
