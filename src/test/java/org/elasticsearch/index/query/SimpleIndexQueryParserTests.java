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

package org.elasticsearch.index.query;


import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queries.*;
import org.apache.lucene.sandbox.queries.FuzzyLikeThisQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.*;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.search.child.CustomQueryWrappingFilter;
import org.elasticsearch.index.search.child.ParentConstantScoreQuery;
import org.elasticsearch.index.search.geo.GeoDistanceFilter;
import org.elasticsearch.index.search.geo.GeoPolygonFilter;
import org.elasticsearch.index.search.geo.InMemoryGeoBoundingBoxFilter;
import org.elasticsearch.index.search.morelikethis.MoreLikeThisFetchService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.RegexpFlag.*;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.factorFunction;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBooleanSubQuery;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleIndexQueryParserTests extends ElasticsearchSingleNodeTest {

    private IndexQueryParserService queryParser;

    @Before
    public void setup() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.filter.type", "none")
                .put("name", "SimpleIndexQueryParserTests")
                .build();
        IndexService indexService = createIndex("test", settings);
        MapperService mapperService = indexService.mapperService();

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/mapping.json");
        mapperService.merge("person", new CompressedString(mapping), true);
        mapperService.documentMapper("person").parse(new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/query/data.json")));

        queryParser = indexService.queryParserService();
    }

    private IndexQueryParserService queryParser() throws IOException {
        return this.queryParser;
    }

    private BytesRef longToPrefixCoded(long val, int shift) {
        BytesRefBuilder bytesRef = new BytesRefBuilder();
        NumericUtils.longToPrefixCoded(val, shift, bytesRef);
        return bytesRef.get();
    }

    @Test
    public void testQueryStringBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").defaultField("content").phraseSlop(1)).query();

        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        assertThat(termQuery.getTerm(), equalTo(new Term("content", "test")));
    }

    @Test
    public void testQueryString() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        assertThat(termQuery.getTerm(), equalTo(new Term("content", "test")));
    }

    @Test
    public void testQueryStringBoostsBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        QueryStringQueryBuilder builder = queryString("field:boosted^2");
        Query parsedQuery = queryParser.parse(builder).query();
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) parsedQuery).getTerm(), equalTo(new Term("field", "boosted")));
        assertThat(parsedQuery.getBoost(), equalTo(2.0f));
        builder.boost(2.0f);
        parsedQuery = queryParser.parse(builder).query();
        assertThat(parsedQuery.getBoost(), equalTo(4.0f));

        builder = queryString("((field:boosted^2) AND (field:foo^1.5))^3");
        parsedQuery = queryParser.parse(builder).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 0).getTerm(), equalTo(new Term("field", "boosted")));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 0).getBoost(), equalTo(2.0f));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 1).getTerm(), equalTo(new Term("field", "foo")));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 1).getBoost(), equalTo(1.5f));
        assertThat(parsedQuery.getBoost(), equalTo(3.0f));
        builder.boost(2.0f);
        parsedQuery = queryParser.parse(builder).query();
        assertThat(parsedQuery.getBoost(), equalTo(6.0f));
    }

    @Test
    public void testQueryStringFields1Builder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content").field("name").useDisMax(false)).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 0).getTerm(), equalTo(new Term("content", "test")));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 1).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test
    public void testQueryStringFields1() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query-fields1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 0).getTerm(), equalTo(new Term("content", "test")));
        assertThat(assertBooleanSubQuery(parsedQuery, TermQuery.class, 1).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test
    public void testQueryStringFieldsMatch() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query-fields-match.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertEquals(Sets.newHashSet(new Term("name.first", "test"), new Term("name.last", "test")),
                Sets.newHashSet(assertBooleanSubQuery(parsedQuery, TermQuery.class, 0).getTerm(),
                        assertBooleanSubQuery(parsedQuery, TermQuery.class, 1).getTerm()));
    }

    @Test
    public void testQueryStringFields2Builder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content").field("name").useDisMax(true)).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test
    public void testQueryStringFields2() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query-fields2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test
    public void testQueryStringFields3Builder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content", 2.2f).field("name").useDisMax(true)).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat((double) disjuncts.get(0).getBoost(), closeTo(2.2, 0.01));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
        assertThat((double) disjuncts.get(1).getBoost(), closeTo(1, 0.01));
    }

    @Test
    public void testQueryStringFields3() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query-fields3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat((double) disjuncts.get(0).getBoost(), closeTo(2.2, 0.01));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
        assertThat((double) disjuncts.get(1).getBoost(), closeTo(1, 0.01));
    }

    @Test
    public void testMatchAllBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(matchAllQuery().boost(1.2f)).query();
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
        MatchAllDocsQuery matchAllDocsQuery = (MatchAllDocsQuery) parsedQuery;
        assertThat((double) matchAllDocsQuery.getBoost(), closeTo(1.2, 0.01));
    }

    @Test
    public void testMatchAll() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/matchAll.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
        MatchAllDocsQuery matchAllDocsQuery = (MatchAllDocsQuery) parsedQuery;
        assertThat((double) matchAllDocsQuery.getBoost(), closeTo(1.2, 0.01));
    }

    @Test
    public void testMatchAllEmpty1() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/match_all_empty1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, equalTo(Queries.newMatchAllQuery()));
        assertThat(parsedQuery, not(sameInstance(Queries.newMatchAllQuery())));
    }

    @Test
    public void testMatchAllEmpty2() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/match_all_empty2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, equalTo(Queries.newMatchAllQuery()));
        assertThat(parsedQuery, not(sameInstance(Queries.newMatchAllQuery())));

    }

    @Test
    public void testStarColonStar() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/starColonStar.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) parsedQuery;
        Filter internalFilter = constantScoreQuery.getFilter();
        assertThat(internalFilter, instanceOf(MatchAllDocsFilter.class));
    }

    @Test
    public void testDisMaxBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(disMaxQuery().boost(1.2f).tieBreaker(0.7f).add(termQuery("name.first", "first")).add(termQuery("name.last", "last"))).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        assertThat((double) disjunctionMaxQuery.getBoost(), closeTo(1.2, 0.01));

        List<Query> disjuncts = disjunctionMaxQuery.getDisjuncts();
        assertThat(disjuncts.size(), equalTo(2));

        Query firstQ = disjuncts.get(0);
        assertThat(firstQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) firstQ).getTerm(), equalTo(new Term("name.first", "first")));

        Query secondsQ = disjuncts.get(1);
        assertThat(secondsQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) secondsQ).getTerm(), equalTo(new Term("name.last", "last")));
    }

    @Test
    public void testDisMax() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/disMax.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        assertThat((double) disjunctionMaxQuery.getBoost(), closeTo(1.2, 0.01));

        List<Query> disjuncts = disjunctionMaxQuery.getDisjuncts();
        assertThat(disjuncts.size(), equalTo(2));

        Query firstQ = disjuncts.get(0);
        assertThat(firstQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) firstQ).getTerm(), equalTo(new Term("name.first", "first")));

        Query secondsQ = disjuncts.get(1);
        assertThat(secondsQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) secondsQ).getTerm(), equalTo(new Term("name.last", "last")));
    }

    @Test
    public void testDisMax2() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/disMax2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;

        List<Query> disjuncts = disjunctionMaxQuery.getDisjuncts();
        assertThat(disjuncts.size(), equalTo(1));

        PrefixQuery firstQ = (PrefixQuery) disjuncts.get(0);
        // since age is automatically registered in data, we encode it as numeric
        assertThat(firstQ.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) firstQ.getBoost(), closeTo(1.2, 0.00001));
    }

    @Test
    public void testTermQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(termQuery("age", 34).buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test
    public void testTermQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/term.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test
    public void testFuzzyQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyQuery("name.first", "sh").buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testFuzzyQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fuzzy.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testFuzzyQueryWithFieldsBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyQuery("name.first", "sh").fuzziness(Fuzziness.fromSimilarity(0.1f)).prefixLength(1).boost(2.0f).buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(FuzzyQuery.floatToEdits(0.1f, "sh".length())));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
        assertThat(fuzzyQuery.getBoost(), equalTo(2.0f));
    }

    @Test
    public void testFuzzyQueryWithFields() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fuzzy-with-fields.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(FuzzyQuery.floatToEdits(0.1f, "sh".length())));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
        assertThat(fuzzyQuery.getBoost(), equalTo(2.0f));
    }

    @Test
    public void testFuzzyQueryWithFields2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fuzzy-with-fields2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fuzzyQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fuzzyQuery.getMin().longValue(), equalTo(7l));
        assertThat(fuzzyQuery.getMax().longValue(), equalTo(17l));
    }

    @Test
    public void testTermWithBoostQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(termQuery("age", 34).boost(2.0f)).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
        assertThat((double) fieldQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test
    public void testTermWithBoostQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/term-with-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
        assertThat((double) fieldQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test
    public void testPrefixQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("name.first", "sh")).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testPrefixQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/prefix.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testPrefixBoostQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/prefix-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(1.2, 0.00001));
    }

    @Test
    public void testPrefixFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), prefixFilter("name.first", "sh"))).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testPrefixFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/prefix-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testPrefixNamedFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/prefix-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery.query();
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test
    public void testPrefixQueryBoostQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("name.first", "sh").boost(2.0f)).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test
    public void testPrefixQueryBoostQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/prefix-with-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test
    public void testPrefixQueryWithUnknownField() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("unknown", "sh")).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("unknown", "sh")));
        assertThat(prefixQuery.getRewriteMethod(), notNullValue());
    }

    @Test
    public void testRegexpQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(regexpQuery("name.first", "s.*y")).query();
        assertThat(parsedQuery, instanceOf(RegexpQuery.class));
        RegexpQuery regexpQuery = (RegexpQuery) parsedQuery;
        assertThat(regexpQuery.getField(), equalTo("name.first"));
    }

    @Test
    public void testRegexpQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/regexp.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(RegexpQuery.class));
        RegexpQuery regexpQuery = (RegexpQuery) parsedQuery;
        assertThat(regexpQuery.getField(), equalTo("name.first"));
    }

    @Test
    public void testRegexpFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/regexp-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(RegexpFilter.class));
        RegexpFilter regexpFilter = (RegexpFilter) filter;
        assertThat(regexpFilter.field(), equalTo("name.first"));
        assertThat(regexpFilter.regexp(), equalTo("s.*y"));
    }

    @Test
    public void testNamedRegexpFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/regexp-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery.query()).getFilter();
        assertThat(filter, instanceOf(RegexpFilter.class));
        RegexpFilter regexpFilter = (RegexpFilter) filter;
        assertThat(regexpFilter.field(), equalTo("name.first"));
        assertThat(regexpFilter.regexp(), equalTo("s.*y"));
    }

    @Test
    public void testRegexpWithFlagsFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/regexp-filter-flags.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery.query()).getFilter();
        assertThat(filter, instanceOf(RegexpFilter.class));
        RegexpFilter regexpFilter = (RegexpFilter) filter;
        assertThat(regexpFilter.field(), equalTo("name.first"));
        assertThat(regexpFilter.regexp(), equalTo("s.*y"));
        assertThat(regexpFilter.flags(), equalTo(INTERSECTION.value() | COMPLEMENT.value() | EMPTY.value()));
    }

    @Test
    public void testNamedAndCachedRegexpWithFlagsFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/regexp-filter-flags-named-cached.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery.query()).getFilter();
        assertThat(filter, instanceOf(CacheKeyFilter.Wrapper.class));
        CacheKeyFilter.Wrapper wrapper = (CacheKeyFilter.Wrapper) filter;
        assertThat(new BytesRef(wrapper.cacheKey().bytes()).utf8ToString(), equalTo("key"));
        assertThat(wrapper.wrappedFilter(), instanceOf(RegexpFilter.class));
        RegexpFilter regexpFilter = (RegexpFilter) wrapper.wrappedFilter();
        assertThat(regexpFilter.field(), equalTo("name.first"));
        assertThat(regexpFilter.regexp(), equalTo("s.*y"));
        assertThat(regexpFilter.flags(), equalTo(INTERSECTION.value() | COMPLEMENT.value() | EMPTY.value()));
    }

    @Test
    public void testRegexpBoostQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/regexp-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(RegexpQuery.class));
        RegexpQuery regexpQuery = (RegexpQuery) parsedQuery;
        assertThat(regexpQuery.getField(), equalTo("name.first"));
        assertThat(regexpQuery.getBoost(), equalTo(1.2f));
    }

    @Test
    public void testWildcardQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(wildcardQuery("name.first", "sh*")).query();
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
    }

    @Test
    public void testWildcardQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/wildcard.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
    }

    @Test
    public void testWildcardBoostQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/wildcard-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
        assertThat((double) wildcardQuery.getBoost(), closeTo(1.2, 0.00001));
    }

    @Test
    public void testRangeQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(rangeQuery("age").from(23).to(54).includeLower(true).includeUpper(false)).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery rangeQuery = (NumericRangeQuery) parsedQuery;
        assertThat(rangeQuery.getField(), equalTo("age"));
        assertThat(rangeQuery.getMin().intValue(), equalTo(23));
        assertThat(rangeQuery.getMax().intValue(), equalTo(54));
        assertThat(rangeQuery.includesMin(), equalTo(true));
        assertThat(rangeQuery.includesMax(), equalTo(false));
    }

    @Test
    public void testRangeQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/range.json");
        Query parsedQuery = queryParser.parse(query).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery rangeQuery = (NumericRangeQuery) parsedQuery;
        assertThat(rangeQuery.getField(), equalTo("age"));
        assertThat(rangeQuery.getMin().intValue(), equalTo(23));
        assertThat(rangeQuery.getMax().intValue(), equalTo(54));
        assertThat(rangeQuery.includesMin(), equalTo(true));
        assertThat(rangeQuery.includesMax(), equalTo(false));
    }

    @Test
    public void testRange2Query() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/range2.json");
        Query parsedQuery = queryParser.parse(query).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery rangeQuery = (NumericRangeQuery) parsedQuery;
        assertThat(rangeQuery.getField(), equalTo("age"));
        assertThat(rangeQuery.getMin().intValue(), equalTo(23));
        assertThat(rangeQuery.getMax().intValue(), equalTo(54));
        assertThat(rangeQuery.includesMin(), equalTo(true));
        assertThat(rangeQuery.includesMax(), equalTo(false));
    }

    @Test
    public void testRangeFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), rangeFilter("age").from(23).to(54).includeLower(true).includeUpper(false))).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
        assertThat(rangeFilter.includesMin(), equalTo(true));
        assertThat(rangeFilter.includesMax(), equalTo(false));
    }

    @Test
    public void testRangeFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/range-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
        assertThat(rangeFilter.includesMin(), equalTo(true));
        assertThat(rangeFilter.includesMax(), equalTo(false));
    }

    @Test
    public void testRangeNamedFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/range-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery.query()).getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
        assertThat(rangeFilter.includesMin(), equalTo(true));
        assertThat(rangeFilter.includesMax(), equalTo(false));
    }

    @Test
    public void testRangeFilteredQueryBuilder_executionFieldData() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), rangeFilter("age").from(23).to(54).includeLower(true).includeUpper(false).setExecution("fielddata"))).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        Filter filter = ((XFilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFieldDataFilter.class));
        NumericRangeFieldDataFilter<Number> rangeFilter = (NumericRangeFieldDataFilter<Number>) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getLowerVal().intValue(), equalTo(23));
        assertThat(rangeFilter.getUpperVal().intValue(), equalTo(54));
        assertThat(rangeFilter.isIncludeLower(), equalTo(true));
        assertThat(rangeFilter.isIncludeUpper(), equalTo(false));
    }

    @Test
    public void testBoolFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), boolFilter().must(termFilter("name.first", "shay1"), termFilter("name.first", "shay4")).mustNot(termFilter("name.first", "shay2")).should(termFilter("name.first", "shay3")))).query();

        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        XBooleanFilter booleanFilter = (XBooleanFilter) filteredQuery.getFilter();

        Iterator<FilterClause> iterator = booleanFilter.iterator();
        assertThat(iterator.hasNext(), equalTo(true));
        FilterClause clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay1")));

        assertThat(iterator.hasNext(), equalTo(true));
        clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay4")));

        assertThat(iterator.hasNext(), equalTo(true));
        clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay2")));

        assertThat(iterator.hasNext(), equalTo(true));
        clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay3")));

        assertThat(iterator.hasNext(), equalTo(false));
    }


    @Test
    public void testBoolFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/bool-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        XBooleanFilter booleanFilter = (XBooleanFilter) filteredQuery.getFilter();

        Iterator<FilterClause> iterator = booleanFilter.iterator();
        assertThat(iterator.hasNext(), equalTo(true));
        FilterClause clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay1")));

        assertThat(iterator.hasNext(), equalTo(true));
        clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay4")));

        assertThat(iterator.hasNext(), equalTo(true));
        clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay2")));

        assertThat(iterator.hasNext(), equalTo(true));
        clause = iterator.next();
        assertThat(clause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(((TermFilter) clause.getFilter()).getTerm(), equalTo(new Term("name.first", "shay3")));

        assertThat(iterator.hasNext(), equalTo(false));
    }

    @Test
    public void testAndFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(matchAllQuery(), andFilter(termFilter("name.first", "shay1"), termFilter("name.first", "shay4")))).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;

        AndFilter andFilter = (AndFilter) constantScoreQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testAndFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/and-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;

        AndFilter andFilter = (AndFilter) filteredQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testAndNamedFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/and-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery.query();

        AndFilter andFilter = (AndFilter) filteredQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testAndFilteredQuery2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/and-filter2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;

        AndFilter andFilter = (AndFilter) filteredQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testOrFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(matchAllQuery(), orFilter(termFilter("name.first", "shay1"), termFilter("name.first", "shay4")))).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;

        OrFilter andFilter = (OrFilter) constantScoreQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testOrFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/or-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;

        OrFilter orFilter = (OrFilter) filteredQuery.getFilter();
        assertThat(orFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) orFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) orFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testOrFilteredQuery2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/or-filter2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;

        OrFilter orFilter = (OrFilter) filteredQuery.getFilter();
        assertThat(orFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) orFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) orFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test
    public void testNotFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(matchAllQuery(), notFilter(termFilter("name.first", "shay1")))).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;

        NotFilter notFilter = (NotFilter) constantScoreQuery.getFilter();
        assertThat(((TermFilter) notFilter.filter()).getTerm(), equalTo(new Term("name.first", "shay1")));
    }

    @Test
    public void testNotFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/not-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));

        NotFilter notFilter = (NotFilter) filteredQuery.getFilter();
        assertThat(((TermFilter) notFilter.filter()).getTerm(), equalTo(new Term("name.first", "shay1")));
    }

    @Test
    public void testNotFilteredQuery2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/not-filter2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));

        NotFilter notFilter = (NotFilter) filteredQuery.getFilter();
        assertThat(((TermFilter) notFilter.filter()).getTerm(), equalTo(new Term("name.first", "shay1")));
    }

    @Test
    public void testNotFilteredQuery3() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/not-filter3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));

        NotFilter notFilter = (NotFilter) filteredQuery.getFilter();
        assertThat(((TermFilter) notFilter.filter()).getTerm(), equalTo(new Term("name.first", "shay1")));
    }

    @Test
    public void testBoostingQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(boostingQuery().positive(termQuery("field1", "value1")).negative(termQuery("field1", "value2")).negativeBoost(0.2f)).query();
        assertThat(parsedQuery, instanceOf(BoostingQuery.class));
    }

    @Test
    public void testBoostingQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/boosting-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BoostingQuery.class));
    }

    @Test
    public void testQueryStringFuzzyNumeric() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fuzzyQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fuzzyQuery.getMin().longValue(), equalTo(12l));
        assertThat(fuzzyQuery.getMax().longValue(), equalTo(12l));
    }

    @Test
    public void testBoolQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(boolQuery().must(termQuery("content", "test1")).must(termQuery("content", "test4")).mustNot(termQuery("content", "test2")).should(termQuery("content", "test3"))).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) parsedQuery;
        BooleanClause[] clauses = booleanQuery.getClauses();

        assertThat(clauses.length, equalTo(4));

        assertThat(((TermQuery) clauses[0].getQuery()).getTerm(), equalTo(new Term("content", "test1")));
        assertThat(clauses[0].getOccur(), equalTo(BooleanClause.Occur.MUST));

        assertThat(((TermQuery) clauses[1].getQuery()).getTerm(), equalTo(new Term("content", "test4")));
        assertThat(clauses[1].getOccur(), equalTo(BooleanClause.Occur.MUST));

        assertThat(((TermQuery) clauses[2].getQuery()).getTerm(), equalTo(new Term("content", "test2")));
        assertThat(clauses[2].getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));

        assertThat(((TermQuery) clauses[3].getQuery()).getTerm(), equalTo(new Term("content", "test3")));
        assertThat(clauses[3].getOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }


    @Test
    public void testBoolQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/bool.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) parsedQuery;
        BooleanClause[] clauses = booleanQuery.getClauses();

        assertThat(clauses.length, equalTo(4));

        assertThat(((TermQuery) clauses[0].getQuery()).getTerm(), equalTo(new Term("content", "test1")));
        assertThat(clauses[0].getOccur(), equalTo(BooleanClause.Occur.MUST));

        assertThat(((TermQuery) clauses[1].getQuery()).getTerm(), equalTo(new Term("content", "test4")));
        assertThat(clauses[1].getOccur(), equalTo(BooleanClause.Occur.MUST));

        assertThat(((TermQuery) clauses[2].getQuery()).getTerm(), equalTo(new Term("content", "test2")));
        assertThat(clauses[2].getOccur(), equalTo(BooleanClause.Occur.MUST_NOT));

        assertThat(((TermQuery) clauses[3].getQuery()).getTerm(), equalTo(new Term("content", "test3")));
        assertThat(clauses[3].getOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }

    @Test
    public void testTermsQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(termsQuery("name.first", Lists.newArrayList("shay", "test"))).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) parsedQuery;
        BooleanClause[] clauses = booleanQuery.getClauses();

        assertThat(clauses.length, equalTo(2));

        assertThat(((TermQuery) clauses[0].getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(clauses[0].getOccur(), equalTo(BooleanClause.Occur.SHOULD));

        assertThat(((TermQuery) clauses[1].getQuery()).getTerm(), equalTo(new Term("name.first", "test")));
        assertThat(clauses[1].getOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }

    @Test
    public void testTermsQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/terms-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) parsedQuery;
        BooleanClause[] clauses = booleanQuery.getClauses();

        assertThat(clauses.length, equalTo(2));

        assertThat(((TermQuery) clauses[0].getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(clauses[0].getOccur(), equalTo(BooleanClause.Occur.SHOULD));

        assertThat(((TermQuery) clauses[1].getQuery()).getTerm(), equalTo(new Term("name.first", "test")));
        assertThat(clauses[1].getOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }

    @Test
    public void testInQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(termsQuery("name.first", Lists.newArrayList("test1", "test2", "test3"))).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) parsedQuery;
        BooleanClause[] clauses = booleanQuery.getClauses();

        assertThat(clauses.length, equalTo(3));

        assertThat(((TermQuery) clauses[0].getQuery()).getTerm(), equalTo(new Term("name.first", "test1")));
        assertThat(clauses[0].getOccur(), equalTo(BooleanClause.Occur.SHOULD));

        assertThat(((TermQuery) clauses[1].getQuery()).getTerm(), equalTo(new Term("name.first", "test2")));
        assertThat(clauses[1].getOccur(), equalTo(BooleanClause.Occur.SHOULD));

        assertThat(((TermQuery) clauses[2].getQuery()).getTerm(), equalTo(new Term("name.first", "test3")));
        assertThat(clauses[2].getOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }

    @Test
    public void testFilteredQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), termFilter("name.last", "banon"))).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testFilteredQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/filtered-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testFilteredQuery2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/filtered-query2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testFilteredQuery3() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/filtered-query3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));

        Filter filter = filteredQuery.getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
    }

    @Test
    public void testFilteredQuery4() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/filtered-query4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        WildcardQuery wildcardQuery = (WildcardQuery) filteredQuery.getQuery();
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
        assertThat((double) wildcardQuery.getBoost(), closeTo(1.1, 0.001));

        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testLimitFilter() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/limit-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(LimitFilter.class));
        assertThat(((LimitFilter) filteredQuery.getFilter()).getLimit(), equalTo(2));

        assertThat(filteredQuery.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
    }

    @Test
    public void testTermFilterQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/term-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(TermFilter.class));
        TermFilter termFilter = (TermFilter) filteredQuery.getFilter();
        assertThat(termFilter.getTerm().field(), equalTo("name.last"));
        assertThat(termFilter.getTerm().text(), equalTo("banon"));
    }

    @Test
    public void testTermNamedFilterQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/term-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery.query();
        assertThat(filteredQuery.getFilter(), instanceOf(TermFilter.class));
        TermFilter termFilter = (TermFilter) filteredQuery.getFilter();
        assertThat(termFilter.getTerm().field(), equalTo("name.last"));
        assertThat(termFilter.getTerm().text(), equalTo("banon"));
    }

    @Test
    public void testTermsFilterQueryBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), termsFilter("name.last", "banon", "kimchy"))).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(TermsFilter.class));
        TermsFilter termsFilter = (TermsFilter) filteredQuery.getFilter();
        //assertThat(termsFilter.getTerms().length, equalTo(2));
        //assertThat(termsFilter.getTerms()[0].text(), equalTo("banon"));
    }


    @Test
    public void testTermsFilterQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/terms-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(TermsFilter.class));
        TermsFilter termsFilter = (TermsFilter) filteredQuery.getFilter();
        //assertThat(termsFilter.getTerms().length, equalTo(2));
        //assertThat(termsFilter.getTerms()[0].text(), equalTo("banon"));
    }

    @Test
    public void testTermsWithNameFilterQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/terms-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery.query();
        assertThat(filteredQuery.getFilter(), instanceOf(TermsFilter.class));
        TermsFilter termsFilter = (TermsFilter) filteredQuery.getFilter();
        //assertThat(termsFilter.getTerms().length, equalTo(2));
        //assertThat(termsFilter.getTerms()[0].text(), equalTo("banon"));
    }

    @Test
    public void testConstantScoreQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(constantScoreQuery(termFilter("name.last", "banon"))).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        assertThat(((TermFilter) constantScoreQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testConstantScoreQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/constantScore-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        assertThat(((TermFilter) constantScoreQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testCustomBoostFactorQueryBuilder_withFunctionScore() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(functionScoreQuery(termQuery("name.last", "banon"), factorFunction(1.3f))).query();
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term("name.last", "banon")));
        assertThat((double) ((BoostScoreFunction) functionScoreQuery.getFunction()).getBoost(), closeTo(1.3, 0.001));
    }

    @Test
    public void testCustomBoostFactorQueryBuilder_withFunctionScoreWithoutQueryGiven() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(functionScoreQuery(factorFunction(1.3f))).query();
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(functionScoreQuery.getSubQuery() instanceof XConstantScoreQuery, equalTo(true));
        assertThat(((XConstantScoreQuery) functionScoreQuery.getSubQuery()).getFilter() instanceof MatchAllDocsFilter, equalTo(true));
        assertThat((double) ((BoostScoreFunction) functionScoreQuery.getFunction()).getBoost(), closeTo(1.3, 0.001));
    }

    @Test
    public void testSpanTermQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanTermQuery("age", 34)).query();
        assertThat(parsedQuery, instanceOf(SpanTermQuery.class));
        SpanTermQuery termQuery = (SpanTermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
    }

    @Test
    public void testSpanTermQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanTerm.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanTermQuery.class));
        SpanTermQuery termQuery = (SpanTermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
    }

    @Test
    public void testSpanNotQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanNotQuery().include(spanTermQuery("age", 34)).exclude(spanTermQuery("age", 35))).query();
        assertThat(parsedQuery, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanNotQuery.getInclude()).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanNotQuery.getExclude()).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
    }

    @Test
    public void testSpanNotQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanNot.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanNotQuery.getInclude()).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanNotQuery.getExclude()).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
    }

    @Test
    public void testSpanFirstQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanFirstQuery(spanTermQuery("age", 34), 12)).query();
        assertThat(parsedQuery, instanceOf(SpanFirstQuery.class));
        SpanFirstQuery spanFirstQuery = (SpanFirstQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanFirstQuery.getMatch()).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(spanFirstQuery.getEnd(), equalTo(12));
    }

    @Test
    public void testSpanFirstQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanFirst.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanFirstQuery.class));
        SpanFirstQuery spanFirstQuery = (SpanFirstQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanFirstQuery.getMatch()).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(spanFirstQuery.getEnd(), equalTo(12));
    }

    @Test
    public void testSpanNearQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanNearQuery().clause(spanTermQuery("age", 34)).clause(spanTermQuery("age", 35)).clause(spanTermQuery("age", 36)).slop(12).inOrder(false).collectPayloads(false)).query();
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", longToPrefixCoded(36, 0))));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }

    @Test
    public void testSpanNearQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanNear.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", longToPrefixCoded(36, 0))));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }

    @Test
    public void testFieldMaskingSpanQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanFieldMaskingTerm.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
        assertThat(((SpanTermQuery) ((FieldMaskingSpanQuery) spanNearQuery.getClauses()[2]).getMaskedQuery()).getTerm(), equalTo(new Term("age_1", "36")));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }


    @Test
    public void testSpanOrQueryBuilder() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanOrQuery().clause(spanTermQuery("age", 34)).clause(spanTermQuery("age", 35)).clause(spanTermQuery("age", 36))).query();
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", longToPrefixCoded(36, 0))));
    }

    @Test
    public void testSpanOrQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanOr.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", longToPrefixCoded(36, 0))));
    }

    @Test
    public void testSpanOrQuery2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/spanOr2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", longToPrefixCoded(34, 0))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", longToPrefixCoded(35, 0))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", longToPrefixCoded(36, 0))));
    }

    @Test
    public void testSpanMultiTermWildcardQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/span-multi-term-wildcard.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanMultiTermQueryWrapper.class));
        WildcardQuery expectedWrapped = new WildcardQuery(new Term("user", "ki*y"));
        expectedWrapped.setBoost(1.08f);
        SpanMultiTermQueryWrapper<MultiTermQuery> wrapper = (SpanMultiTermQueryWrapper<MultiTermQuery>) parsedQuery;
        assertThat(wrapper, equalTo(new SpanMultiTermQueryWrapper<MultiTermQuery>(expectedWrapped)));
    }

    @Test
    public void testSpanMultiTermPrefixQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/span-multi-term-prefix.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanMultiTermQueryWrapper.class));
        PrefixQuery expectedWrapped = new PrefixQuery(new Term("user", "ki"));
        expectedWrapped.setBoost(1.08f);
        SpanMultiTermQueryWrapper<MultiTermQuery> wrapper = (SpanMultiTermQueryWrapper<MultiTermQuery>) parsedQuery;
        assertThat(wrapper, equalTo(new SpanMultiTermQueryWrapper<MultiTermQuery>(expectedWrapped)));
    }

    @Test
    public void testSpanMultiTermFuzzyTermQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/span-multi-term-fuzzy-term.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanMultiTermQueryWrapper.class));
        SpanMultiTermQueryWrapper<MultiTermQuery> wrapper = (SpanMultiTermQueryWrapper<MultiTermQuery>) parsedQuery;
        assertThat(wrapper.getField(), equalTo("user"));
    }

    @Test
    public void testSpanMultiTermFuzzyRangeQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/span-multi-term-fuzzy-range.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanMultiTermQueryWrapper.class));
        NumericRangeQuery<Long> expectedWrapped = NumericRangeQuery.newLongRange("age", NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, 7l, 17l, true, true);
        expectedWrapped.setBoost(2.0f);
        SpanMultiTermQueryWrapper<MultiTermQuery> wrapper = (SpanMultiTermQueryWrapper<MultiTermQuery>) parsedQuery;
        assertThat(wrapper, equalTo(new SpanMultiTermQueryWrapper<MultiTermQuery>(expectedWrapped)));
    }

    @Test
    public void testSpanMultiTermNumericRangeQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/span-multi-term-range-numeric.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanMultiTermQueryWrapper.class));
        NumericRangeQuery<Long> expectedWrapped = NumericRangeQuery.newLongRange("age", NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT, 10l, 20l, true, false);
        expectedWrapped.setBoost(2.0f);
        SpanMultiTermQueryWrapper<MultiTermQuery> wrapper = (SpanMultiTermQueryWrapper<MultiTermQuery>) parsedQuery;
        assertThat(wrapper, equalTo(new SpanMultiTermQueryWrapper<MultiTermQuery>(expectedWrapped)));
    }

    @Test
    public void testSpanMultiTermTermRangeQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/span-multi-term-range-term.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanMultiTermQueryWrapper.class));
        TermRangeQuery expectedWrapped = TermRangeQuery.newStringRange("user", "alice", "bob", true, false);
        expectedWrapped.setBoost(2.0f);
        SpanMultiTermQueryWrapper<MultiTermQuery> wrapper = (SpanMultiTermQueryWrapper<MultiTermQuery>) parsedQuery;
        assertThat(wrapper, equalTo(new SpanMultiTermQueryWrapper<MultiTermQuery>(expectedWrapped)));
    }

    @Test
    public void testQueryFilterBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), queryFilter(termQuery("name.last", "banon")))).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testQueryFilter() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/query-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery;
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testFQueryFilter() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fquery-filter.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XFilteredQuery.class));
        XFilteredQuery filteredQuery = (XFilteredQuery) parsedQuery.query();
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test
    public void testMoreLikeThisBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(moreLikeThisQuery("name.first", "name.last").likeText("something").minTermFreq(1).maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test
    public void testMoreLikeThis() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/mlt.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getMoreLikeFields()[1], equalTo("name.last"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test
    public void testMoreLikeThisIds() throws Exception {
        MoreLikeThisQueryParser parser = (MoreLikeThisQueryParser) queryParser.queryParser("more_like_this");
        parser.setFetchService(new MockMoreLikeThisFetchService());

        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/mlt-items.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) parsedQuery;
        assertThat(booleanQuery.getClauses().length, is(1));

        BooleanClause itemClause = booleanQuery.getClauses()[0];
        assertThat(itemClause.getOccur(), is(BooleanClause.Occur.SHOULD));
        assertThat(itemClause.getQuery(), instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) itemClause.getQuery();

        // check each Fields is for each item
        for (int id = 1; id <= 4; id++) {
            Fields fields = mltQuery.getLikeFields()[id - 1];
            assertThat(termsToString(fields.terms("name.first")), is(String.valueOf(id)));
            assertThat(termsToString(fields.terms("name.last")), is(String.valueOf(id)));
        }
    }

    @Test
    public void testMLTPercentTermsToMatch() throws Exception {
        // setup for mocking fetching items
        MoreLikeThisQueryParser parser = (MoreLikeThisQueryParser) queryParser.queryParser("more_like_this");
        parser.setFetchService(new MockMoreLikeThisFetchService());

        // parsing the ES query
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/mlt-items.json");
        BooleanQuery parsedQuery = (BooleanQuery) queryParser.parse(query).query();

        // get MLT query, other clause is for include/exclude items
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery.getClauses()[0].getQuery();

        // all terms must match
        mltQuery.setMinimumShouldMatch("100%");
        mltQuery.setMinWordLen(0);
        mltQuery.setMinDocFreq(0);

        // one document has all values
        MemoryIndex index = new MemoryIndex();
        index.addField("name.first", "apache lucene", new WhitespaceAnalyzer());
        index.addField("name.last", "1 2 3 4", new WhitespaceAnalyzer());

        // two clauses, one for items and one for like_text if set
        BooleanQuery luceneQuery = (BooleanQuery) mltQuery.rewrite(index.createSearcher().getIndexReader());
        BooleanClause[] clauses = luceneQuery.getClauses();

        // check for items
        int minNumberShouldMatch = ((BooleanQuery) (clauses[0].getQuery())).getMinimumNumberShouldMatch();
        assertThat(minNumberShouldMatch, is(4));

        // and for like_text
        minNumberShouldMatch = ((BooleanQuery) (clauses[1].getQuery())).getMinimumNumberShouldMatch();
        assertThat(minNumberShouldMatch, is(2));
    }

    private static class MockMoreLikeThisFetchService extends MoreLikeThisFetchService {

        public MockMoreLikeThisFetchService() {
            super(null, ImmutableSettings.Builder.EMPTY_SETTINGS);
        }

        @Override
        public Fields[] fetch(MultiTermVectorsRequest items) throws IOException {
            List<Fields> likeTexts = new ArrayList<>();
            for (TermVectorRequest item : items) {
                likeTexts.add(generateFields(item.selectedFields().toArray(Strings.EMPTY_ARRAY), item.id()));
            }
            return likeTexts.toArray(Fields.EMPTY_ARRAY);
        }
    }

    private static Fields generateFields(String[] fieldNames, String text) throws IOException {
        MemoryIndex index = new MemoryIndex();
        for (String fieldName : fieldNames) {
            index.addField(fieldName, text, new WhitespaceAnalyzer(Lucene.VERSION));
        }
        return MultiFields.getFields(index.createSearcher().getIndexReader());
    }

    private static String termsToString(Terms terms) throws IOException {
        String strings = "";
        TermsEnum termsEnum = terms.iterator(null);
        CharsRefBuilder spare = new CharsRefBuilder();
        BytesRef text;
        while((text = termsEnum.next()) != null) {
            spare.copyUTF8Bytes(text);
            String term = spare.toString();
            strings += term;
        }
        return strings;
    }

    @Test
    public void testFuzzyLikeThisBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyLikeThisQuery("name.first", "name.last").likeText("something").maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
        parsedQuery = queryParser.parse(fuzzyLikeThisQuery("name.first", "name.last").likeText("something").maxQueryTerms(12).fuzziness(Fuzziness.build("4"))).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));

        Query parsedQuery1 = queryParser.parse(fuzzyLikeThisQuery("name.first", "name.last").likeText("something").maxQueryTerms(12).fuzziness(Fuzziness.build("4.0"))).query();
        assertThat(parsedQuery1, instanceOf(FuzzyLikeThisQuery.class));
        assertThat(parsedQuery, equalTo(parsedQuery1));

        try {
            queryParser.parse(fuzzyLikeThisQuery("name.first", "name.last").likeText("something").maxQueryTerms(12).fuzziness(Fuzziness.build("4.1"))).query();
            fail("exception expected - fractional edit distance");
        } catch (ElasticsearchException ex) {
           //
        }

        try {
            queryParser.parse(fuzzyLikeThisQuery("name.first", "name.last").likeText("something").maxQueryTerms(12).fuzziness(Fuzziness.build("-" + between(1, 100)))).query();
            fail("exception expected - negative edit distance");
        } catch (ElasticsearchException ex) {
            //
        }
        String[] queries = new String[] {
                "{\"flt\": {\"fields\": [\"comment\"], \"like_text\": \"FFFdfds\",\"fuzziness\": \"4\"}}",
                "{\"flt\": {\"fields\": [\"comment\"], \"like_text\": \"FFFdfds\",\"fuzziness\": \"4.00000000\"}}",
                "{\"flt\": {\"fields\": [\"comment\"], \"like_text\": \"FFFdfds\",\"fuzziness\": \"4.\"}}",
                "{\"flt\": {\"fields\": [\"comment\"], \"like_text\": \"FFFdfds\",\"fuzziness\": 4}}",
                "{\"flt\": {\"fields\": [\"comment\"], \"like_text\": \"FFFdfds\",\"fuzziness\": 4.0}}"
        };
        int iters = scaledRandomIntBetween(5, 100);
        for (int i = 0; i < iters; i++) {
            parsedQuery = queryParser.parse(new BytesArray((String) randomFrom(queries))).query();
            parsedQuery1 = queryParser.parse(new BytesArray((String) randomFrom(queries))).query();
            assertThat(parsedQuery1, instanceOf(FuzzyLikeThisQuery.class));
            assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
            assertThat(parsedQuery, equalTo(parsedQuery1));
        }
    }

    @Test
    public void testFuzzyLikeThis() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fuzzyLikeThis.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test
    public void testFuzzyLikeFieldThisBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyLikeThisFieldQuery("name.first").likeText("something").maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test
    public void testFuzzyLikeThisField() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fuzzyLikeThisField.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test
    public void testMoreLikeThisFieldBuilder() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        Query parsedQuery = queryParser.parse(moreLikeThisFieldQuery("name.first").likeText("something").minTermFreq(1).maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test
    public void testMoreLikeThisField() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/mltField.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test
    public void testGeoDistanceFilterNamed() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery.query();
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter1() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter3() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter4() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter5() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance5.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter6() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance6.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter7() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance7.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(0.012, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter8() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance8.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.KILOMETERS.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter9() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance9.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter10() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance10.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter11() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance11.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoDistanceFilter12() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_distance12.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(DistanceUnit.DEFAULT.convert(12, DistanceUnit.MILES), 0.00001));
    }

    @Test
    public void testGeoBoundingBoxFilterNamed() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.query(), instanceOf(XConstantScoreQuery.class));
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery.query();
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }


    @Test
    public void testGeoBoundingBoxFilter1() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }

    @Test
    public void testGeoBoundingBoxFilter2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }

    @Test
    public void testGeoBoundingBoxFilter3() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }

    @Test
    public void testGeoBoundingBoxFilter4() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }

    @Test
    public void testGeoBoundingBoxFilter5() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox5.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }

    @Test
    public void testGeoBoundingBoxFilter6() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_boundingbox6.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        InMemoryGeoBoundingBoxFilter filter = (InMemoryGeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat(), closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon(), closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat(), closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon(), closeTo(-80, 0.00001));
    }


    @Test
    public void testGeoPolygonNamedFilter() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_polygon-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery.query();
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(4));
        assertThat(filter.points()[0].lat(), closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon(), closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat(), closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon(), closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat(), closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon(), closeTo(-90, 0.00001));
    }


    @Test
    public void testGeoPolygonFilterParsingExceptions() throws IOException {
        String[] brokenFiles = new String[]{
                "/org/elasticsearch/index/query/geo_polygon_exception_1.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_2.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_3.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_4.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_5.json"
        };
        for (String brokenFile : brokenFiles) {
            IndexQueryParserService queryParser = queryParser();
            String query = copyToStringFromClasspath(brokenFile);
            try {
                queryParser.parse(query).query();
                fail("parsing a broken geo_polygon filter didn't fail as expected while parsing: " + brokenFile);
            } catch (QueryParsingException e) {
                // success!
            }
        }
    }


    @Test
    public void testGeoPolygonFilter1() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_polygon1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(4));
        assertThat(filter.points()[0].lat(), closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon(), closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat(), closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon(), closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat(), closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon(), closeTo(-90, 0.00001));
    }

    @Test
    public void testGeoPolygonFilter2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_polygon2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(4));
        assertThat(filter.points()[0].lat(), closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon(), closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat(), closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon(), closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat(), closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon(), closeTo(-90, 0.00001));
    }

    @Test
    public void testGeoPolygonFilter3() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_polygon3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(4));
        assertThat(filter.points()[0].lat(), closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon(), closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat(), closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon(), closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat(), closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon(), closeTo(-90, 0.00001));
    }

    @Test
    public void testGeoPolygonFilter4() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geo_polygon4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(4));
        assertThat(filter.points()[0].lat(), closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon(), closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat(), closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon(), closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat(), closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon(), closeTo(-90, 0.00001));
    }

    @Test
    public void testGeoShapeFilter() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geoShape-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        XConstantScoreQuery constantScoreQuery = (XConstantScoreQuery) parsedQuery;
        assertThat(constantScoreQuery.getFilter(), instanceOf(IntersectsPrefixTreeFilter.class));
    }

    @Test
    public void testGeoShapeQuery() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/geoShape-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery csq = (ConstantScoreQuery) parsedQuery;
        assertThat(csq.getFilter(), instanceOf(IntersectsPrefixTreeFilter.class));
    }

    @Test
    public void testCommonTermsQuery1() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/commonTerms-query1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery ectQuery = (ExtendedCommonTermsQuery) parsedQuery;
        assertThat(ectQuery.getHighFreqMinimumNumberShouldMatchSpec(), nullValue());
        assertThat(ectQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo("2"));
    }

    @Test
    public void testCommonTermsQuery2() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/commonTerms-query2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery ectQuery = (ExtendedCommonTermsQuery) parsedQuery;
        assertThat(ectQuery.getHighFreqMinimumNumberShouldMatchSpec(), equalTo("50%"));
        assertThat(ectQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo("5<20%"));
    }

    @Test
    public void testCommonTermsQuery3() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/commonTerms-query3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery ectQuery = (ExtendedCommonTermsQuery) parsedQuery;
        assertThat(ectQuery.getHighFreqMinimumNumberShouldMatchSpec(), nullValue());
        assertThat(ectQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo("2"));
    }

    @Test(expected = QueryParsingException.class)
    public void assureMalformedThrowsException() throws IOException {
        IndexQueryParserService queryParser;
        queryParser = queryParser();
        String query;
        query = copyToStringFromClasspath("/org/elasticsearch/index/query/faulty-function-score-query.json");
        Query parsedQuery = queryParser.parse(query).query();
    }

    @Test
    public void testFilterParsing() throws IOException {
        IndexQueryParserService queryParser;
        queryParser = queryParser();
        String query;
        query = copyToStringFromClasspath("/org/elasticsearch/index/query/function-filter-score-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat((double) (parsedQuery.getBoost()), Matchers.closeTo(3.0, 1.e-7));
    }

    @Test
    public void testBadTypeMatchQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/match-query-bad-type.json");
        QueryParsingException expectedException = null;
        try {
            queryParser.parse(query).query();
        } catch (QueryParsingException qpe) {
            expectedException = qpe;
        }
        assertThat(expectedException, notNullValue());
    }

    @Test
    public void testMultiMatchQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/multiMatch-query-simple.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
    }

    @Test
    public void testBadTypeMultiMatchQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/multiMatch-query-bad-type.json");
        QueryParsingException expectedException = null;
        try {
            queryParser.parse(query).query();
        } catch (QueryParsingException qpe) {
            expectedException = qpe;
        }
        assertThat(expectedException, notNullValue());
    }

    @Test
    public void testMultiMatchQueryWithFieldsAsString() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/multiMatch-query-fields-as-string.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
    }

    @Test
    public void testSimpleQueryString() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/simple-query-string.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
    }

    @Test
    public void testMatchWithFuzzyTranspositions() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/match-with-fuzzy-transpositions.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        assertThat( ((FuzzyQuery) parsedQuery).getTranspositions(), equalTo(true));
    }

    @Test
    public void testMatchWithoutFuzzyTranspositions() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/match-without-fuzzy-transpositions.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        assertThat( ((FuzzyQuery) parsedQuery).getTranspositions(), equalTo(false));
    }

    // https://github.com/elasticsearch/elasticsearch/issues/7240
    @Test
    public void testEmptyBooleanQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = jsonBuilder().startObject().startObject("bool").endObject().endObject().string();
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
    }

    // https://github.com/elasticsearch/elasticsearch/issues/7240
    @Test
    public void testEmptyBooleanQueryInsideFQuery() throws Exception {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/fquery-with-empty-bool-query.json");
        XContentParser parser = XContentHelper.createParser(new BytesArray(query));
        ParsedFilter parsedQuery = queryParser.parseInnerFilter(parser);
        assertThat(parsedQuery.filter(), instanceOf(QueryWrapperFilter.class));
        assertThat(((QueryWrapperFilter) parsedQuery.filter()).getQuery(), instanceOf(XFilteredQuery.class));
        assertThat(((XFilteredQuery) ((QueryWrapperFilter) parsedQuery.filter()).getQuery()).getFilter(), instanceOf(TermFilter.class));
        TermFilter filter = (TermFilter) ((XFilteredQuery) ((QueryWrapperFilter) parsedQuery.filter()).getQuery()).getFilter();
        assertThat(filter.getTerm().toString(), equalTo("text:apache"));
    }

    @Test
    public void testProperErrorMessageWhenTwoFunctionsDefinedInQueryBody() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/function-score-query-causing-NPE.json");
        try {
            queryParser.parse(query).query();
            fail("FunctionScoreQueryParser should throw an exception here because two functions in body are not allowed.");
        } catch (QueryParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("Use functions[{...},...] if you want to define several functions."));
        }
    }

    @Test
    public void testWeight1fStillProducesWeighFuction() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String queryString = jsonBuilder().startObject()
                .startObject("function_score")
                .startArray("functions")
                .startObject()
                .startObject("field_value_factor")
                .field("field", "popularity")
                .endObject()
                .field("weight", 1.0)
                .endObject()
                .endArray()
                .endObject()
                .endObject().string();
        IndexService indexService = createIndex("testidx", client().admin().indices().prepareCreate("testidx")
                .addMapping("doc",jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("popularity").field("type", "float").endObject()
                        .endObject()
                        .endObject()));
        SearchContext.setCurrent(createSearchContext(indexService));
        Query query = queryParser.parse(queryString).query();
        assertThat(query, instanceOf(FunctionScoreQuery.class));
        assertThat(((FunctionScoreQuery) query).getFunction(), instanceOf(WeightFactorFunction.class));
        SearchContext.removeCurrent();
    }

    @Test
    public void testProperErrorMessagesForMisplacedWeightsAndFunctions() throws IOException {
        IndexQueryParserService queryParser = queryParser();
        String query = jsonBuilder().startObject().startObject("function_score")
                .startArray("functions")
                .startObject().field("weight", 2).field("boost_factor",2).endObject()
                .endArray()
                .endObject().endObject().string();
        try {
            queryParser.parse(query).query();
            fail("Expect exception here because boost_factor must not have a weight");
        } catch (QueryParsingException e) {
            assertThat(e.getDetailedMessage(), containsString(BoostScoreFunction.BOOST_WEIGHT_ERROR_MESSAGE));
        }
        try {
            functionScoreQuery().add(factorFunction(2.0f).setWeight(2.0f));
            fail("Expect exception here because boost_factor must not have a weight");
        } catch (ElasticsearchIllegalArgumentException e) {
            assertThat(e.getDetailedMessage(), containsString(BoostScoreFunction.BOOST_WEIGHT_ERROR_MESSAGE));
        }
        query = jsonBuilder().startObject().startObject("function_score")
                .startArray("functions")
                .startObject().field("boost_factor",2).endObject()
                .endArray()
                .field("weight", 2)
                .endObject().endObject().string();
        try {
            queryParser.parse(query).query();
            fail("Expect exception here because array of functions and one weight in body is not allowed.");
        } catch (QueryParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("You can either define \"functions\":[...] or a single function, not both. Found \"functions\": [...] already, now encountering \"weight\"."));
        }
        query = jsonBuilder().startObject().startObject("function_score")
                .field("weight", 2)
                .startArray("functions")
                .startObject().field("boost_factor",2).endObject()
                .endArray()
                .endObject().endObject().string();
        try {
            queryParser.parse(query).query();
            fail("Expect exception here because array of functions and one weight in body is not allowed.");
        } catch (QueryParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("You can either define \"functions\":[...] or a single function, not both. Found \"weight\" already, now encountering \"functions\": [...]."));
        }
    }

    // https://github.com/elasticsearch/elasticsearch/issues/6722
    public void testEmptyBoolSubClausesIsMatchAll() throws ElasticsearchException, IOException {
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/bool-query-with-empty-clauses-for-parsing.json");
        IndexService indexService = createIndex("testidx", client().admin().indices().prepareCreate("testidx")
                .addMapping("foo")
                .addMapping("test", "_parent", "type=foo"));
        SearchContext.setCurrent(createSearchContext(indexService));
        IndexQueryParserService queryParser = indexService.queryParserService();
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(XConstantScoreQuery.class));
        assertThat(((XConstantScoreQuery) parsedQuery).getFilter(), instanceOf(CustomQueryWrappingFilter.class));
        assertThat(((CustomQueryWrappingFilter) ((XConstantScoreQuery) parsedQuery).getFilter()).getQuery(), instanceOf(ParentConstantScoreQuery.class));
        assertThat(((CustomQueryWrappingFilter) ((XConstantScoreQuery) parsedQuery).getFilter()).getQuery().toString(), equalTo("parent_filter[foo](*:*)"));
        SearchContext.removeCurrent();
    }
}
