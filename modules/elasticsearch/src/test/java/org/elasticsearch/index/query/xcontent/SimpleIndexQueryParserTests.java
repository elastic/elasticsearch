/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.lucene.search.*;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.search.NumericRangeFieldDataFilter;
import org.elasticsearch.index.search.geo.GeoBoundingBoxFilter;
import org.elasticsearch.index.search.geo.GeoDistanceFilter;
import org.elasticsearch.index.search.geo.GeoPolygonFilter;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import static org.elasticsearch.common.io.Streams.*;
import static org.elasticsearch.index.query.xcontent.FilterBuilders.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class SimpleIndexQueryParserTests {

    private IndexQueryParserService queryParser;

    @BeforeClass public void setupQueryParser() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.filter.type", "none")
                .build();
        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new SettingsModule(settings),
                new ThreadPoolModule(settings),
                new ScriptModule(settings),
                new MapperServiceModule(),
                new IndexSettingsModule(index, settings),
                new IndexCacheModule(settings),
                new AnalysisModule(settings),
                new IndexEngineModule(settings),
                new SimilarityModule(settings),
                new IndexQueryParserModule(settings),
                new IndexNameModule(index)
        ).createInjector();

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/mapping.json");
        injector.getInstance(MapperService.class).add("person", mapping);
        injector.getInstance(MapperService.class).documentMapper("person").parse(copyToBytesFromClasspath("/org/elasticsearch/index/query/xcontent/data.json"));
        this.queryParser = injector.getInstance(IndexQueryParserService.class);
    }

    private XContentIndexQueryParser queryParser() throws IOException {
        return (XContentIndexQueryParser) this.queryParser.defaultIndexQueryParser();
    }

    @Test public void testQueryStringBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").defaultField("content").phraseSlop(1)).query();

        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        assertThat(termQuery.getTerm(), equalTo(new Term("content", "test")));
    }

    @Test public void testQueryString() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        assertThat(termQuery.getTerm(), equalTo(new Term("content", "test")));
    }

    @Test public void testQueryStringFields1Builder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content").field("name").useDisMax(false)).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(((TermQuery) bQuery.clauses().get(0).getQuery()).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) bQuery.clauses().get(1).getQuery()).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields1() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/query-fields1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(((TermQuery) bQuery.clauses().get(0).getQuery()).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) bQuery.clauses().get(1).getQuery()).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFieldsMatch() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/query-fields-match.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(((TermQuery) bQuery.clauses().get(0).getQuery()).getTerm(), equalTo(new Term("name.first", "test")));
        assertThat(((TermQuery) bQuery.clauses().get(1).getQuery()).getTerm(), equalTo(new Term("name.last", "test")));
    }

    @Test public void testQueryStringFields2Builder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content").field("name").useDisMax(true)).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields2() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/query-fields2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields3Builder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content", 2.2f).field("name").useDisMax(true)).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat((double) disjuncts.get(0).getBoost(), closeTo(2.2, 0.01));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
        assertThat((double) disjuncts.get(1).getBoost(), closeTo(1, 0.01));
    }

    @Test public void testQueryStringFields3() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/query-fields3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat((double) disjuncts.get(0).getBoost(), closeTo(2.2, 0.01));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
        assertThat((double) disjuncts.get(1).getBoost(), closeTo(1, 0.01));
    }

    @Test public void testMatchAllBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(matchAllQuery().boost(1.2f)).query();
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
        MatchAllDocsQuery matchAllDocsQuery = (MatchAllDocsQuery) parsedQuery;
        assertThat((double) matchAllDocsQuery.getBoost(), closeTo(1.2, 0.01));
    }

    @Test public void testMatchAll() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/matchAll.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
        MatchAllDocsQuery matchAllDocsQuery = (MatchAllDocsQuery) parsedQuery;
        assertThat((double) matchAllDocsQuery.getBoost(), closeTo(1.2, 0.01));
    }

    @Test public void testDisMaxBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(disMaxQuery().boost(1.2f).tieBreaker(0.7f).add(termQuery("name.first", "first")).add(termQuery("name.last", "last"))).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        assertThat((double) disjunctionMaxQuery.getBoost(), closeTo(1.2, 0.01));

        List<Query> disjuncts = Queries.disMaxClauses(disjunctionMaxQuery);
        assertThat(disjuncts.size(), equalTo(2));

        Query firstQ = disjuncts.get(0);
        assertThat(firstQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) firstQ).getTerm(), equalTo(new Term("name.first", "first")));

        Query secondsQ = disjuncts.get(1);
        assertThat(secondsQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) secondsQ).getTerm(), equalTo(new Term("name.last", "last")));
    }

    @Test public void testDisMax() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/disMax.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        assertThat((double) disjunctionMaxQuery.getBoost(), closeTo(1.2, 0.01));

        List<Query> disjuncts = Queries.disMaxClauses(disjunctionMaxQuery);
        assertThat(disjuncts.size(), equalTo(2));

        Query firstQ = disjuncts.get(0);
        assertThat(firstQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) firstQ).getTerm(), equalTo(new Term("name.first", "first")));

        Query secondsQ = disjuncts.get(1);
        assertThat(secondsQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) secondsQ).getTerm(), equalTo(new Term("name.last", "last")));
    }

    @Test public void testDisMax2() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/disMax2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;

        List<Query> disjuncts = Queries.disMaxClauses(disjunctionMaxQuery);
        assertThat(disjuncts.size(), equalTo(1));

        PrefixQuery firstQ = (PrefixQuery) disjuncts.get(0);
        // since age is automatically registered in data, we encode it as numeric
        assertThat(firstQ.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) firstQ.getBoost(), closeTo(1.2, 0.00001));
    }

    @Test public void testTermQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(termQuery("age", 34).buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test public void testTermQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/term.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test public void testFuzzyQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyQuery("name.first", "sh").buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testFuzzyQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/fuzzy.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testFuzzyQueryWithFieldsBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyQuery("name.first", "sh").minSimilarity(0.1f).prefixLength(1).boost(2.0f).buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
        assertThat(fuzzyQuery.getMinSimilarity(), equalTo(0.1f));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
        assertThat(fuzzyQuery.getBoost(), equalTo(2.0f));
    }

    @Test public void testFuzzyQueryWithFields() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/fuzzy-with-fields.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) parsedQuery;
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term("name.first", "sh")));
        assertThat(fuzzyQuery.getMinSimilarity(), equalTo(0.1f));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
        assertThat(fuzzyQuery.getBoost(), equalTo(2.0f));
    }

    @Test public void testFieldQueryBuilder1() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fieldQuery("age", 34).buildAsBytes()).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test public void testFieldQuery1() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/field1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test public void testFieldQuery2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/field2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.getClauses().length, equalTo(2));
        assertThat(((TermQuery) bQuery.getClauses()[0].getQuery()).getTerm().field(), equalTo("name.first"));
        assertThat(((TermQuery) bQuery.getClauses()[0].getQuery()).getTerm().text(), equalTo("something"));
        assertThat(((TermQuery) bQuery.getClauses()[1].getQuery()).getTerm().field(), equalTo("name.first"));
        assertThat(((TermQuery) bQuery.getClauses()[1].getQuery()).getTerm().text(), equalTo("else"));
    }

    @Test public void testFieldQuery3() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/field3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat((double) parsedQuery.getBoost(), closeTo(2.0, 0.01));
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
    }

    @Test public void testTermWithBoostQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(termQuery("age", 34).boost(2.0f)).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
        assertThat((double) fieldQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testTermWithBoostQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/term-with-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(NumericRangeQuery.class));
        NumericRangeQuery fieldQuery = (NumericRangeQuery) parsedQuery;
        assertThat(fieldQuery.getMin().intValue(), equalTo(34));
        assertThat(fieldQuery.getMax().intValue(), equalTo(34));
        assertThat(fieldQuery.includesMax(), equalTo(true));
        assertThat(fieldQuery.includesMin(), equalTo(true));
        assertThat((double) fieldQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testPrefixQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("name.first", "sh")).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/prefix.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixBoostQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/prefix-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(1.2, 0.00001));
    }

    @Test public void testPrefixFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), prefixFilter("name.first", "sh"))).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/prefix-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixNamedFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/prefix-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery.query();
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixQueryBoostQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("name.first", "sh").boost(2.0f)).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testPrefixQueryBoostQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/prefix-with-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testWildcardQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(wildcardQuery("name.first", "sh*")).query();
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
    }

    @Test public void testWildcardQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/wildcard.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
    }

    @Test public void testWildcardBoostQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/wildcard-boost.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
        assertThat((double) wildcardQuery.getBoost(), closeTo(1.2, 0.00001));
    }

    @Test public void testRangeQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
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

    @Test public void testRangeQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/range.json");
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

    @Test public void testRange2Query() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/range2.json");
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

    @Test public void testRangeFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), rangeFilter("age").from(23).to(54).includeLower(true).includeUpper(false))).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        Filter filter = ((FilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
        assertThat(rangeFilter.includesMin(), equalTo(true));
        assertThat(rangeFilter.includesMax(), equalTo(false));
    }

    @Test public void testRangeFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/range-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        // since age is automatically registered in data, we encode it as numeric
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        Filter filter = ((FilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
        assertThat(rangeFilter.includesMin(), equalTo(true));
        assertThat(rangeFilter.includesMax(), equalTo(false));
    }

    @Test public void testRangeNamedFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/range-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(FilteredQuery.class));
        Filter filter = ((FilteredQuery) parsedQuery.query()).getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
        assertThat(rangeFilter.includesMin(), equalTo(true));
        assertThat(rangeFilter.includesMax(), equalTo(false));
    }

    @Test public void testNumericRangeFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), numericRangeFilter("age").from(23).to(54).includeLower(true).includeUpper(false))).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        Filter filter = ((FilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFieldDataFilter.class));
        NumericRangeFieldDataFilter<Number> rangeFilter = (NumericRangeFieldDataFilter<Number>) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getLowerVal().intValue(), equalTo(23));
        assertThat(rangeFilter.getUpperVal().intValue(), equalTo(54));
        assertThat(rangeFilter.isIncludeLower(), equalTo(true));
        assertThat(rangeFilter.isIncludeUpper(), equalTo(false));
    }

    @Test public void testNumericRangeFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/numeric_range-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        Filter filter = ((FilteredQuery) parsedQuery).getFilter();
        assertThat(filter, instanceOf(NumericRangeFieldDataFilter.class));
        NumericRangeFieldDataFilter<Number> rangeFilter = (NumericRangeFieldDataFilter<Number>) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getLowerVal().intValue(), equalTo(23));
        assertThat(rangeFilter.getUpperVal().intValue(), equalTo(54));
        assertThat(rangeFilter.isIncludeLower(), equalTo(true));
        assertThat(rangeFilter.isIncludeUpper(), equalTo(false));
    }

    @Test public void testBoolFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/bool-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        XBooleanFilter booleanFilter = (XBooleanFilter) filteredQuery.getFilter();

        // TODO get the content and test
    }

    @Test public void testAndFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(matchAllQuery(), andFilter(termFilter("name.first", "shay1"), termFilter("name.first", "shay4")))).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;

        AndFilter andFilter = (AndFilter) constantScoreQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testAndFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/and-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;

        AndFilter andFilter = (AndFilter) filteredQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testAndNamedFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/and-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery.query();

        AndFilter andFilter = (AndFilter) filteredQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testAndFilteredQuery2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/and-filter2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;

        AndFilter andFilter = (AndFilter) filteredQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testOrFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(matchAllQuery(), orFilter(termFilter("name.first", "shay1"), termFilter("name.first", "shay4")))).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;

        OrFilter andFilter = (OrFilter) constantScoreQuery.getFilter();
        assertThat(andFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) andFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) andFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testOrFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/or-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;

        OrFilter orFilter = (OrFilter) filteredQuery.getFilter();
        assertThat(orFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) orFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) orFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testOrFilteredQuery2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/or-filter2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;

        OrFilter orFilter = (OrFilter) filteredQuery.getFilter();
        assertThat(orFilter.filters().size(), equalTo(2));
        assertThat(((TermFilter) orFilter.filters().get(0)).getTerm(), equalTo(new Term("name.first", "shay1")));
        assertThat(((TermFilter) orFilter.filters().get(1)).getTerm(), equalTo(new Term("name.first", "shay4")));
    }

    @Test public void testNotFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(matchAllQuery(), notFilter(termFilter("name.first", "shay1")))).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;

        NotFilter notFilter = (NotFilter) constantScoreQuery.getFilter();
        assertThat(((TermFilter) notFilter.filter()).getTerm(), equalTo(new Term("name.first", "shay1")));
    }

    @Test public void testNotFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/not-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;

        NotFilter notFilter = (NotFilter) filteredQuery.getFilter();
        assertThat(((TermFilter) notFilter.filter()).getTerm(), equalTo(new Term("name.first", "shay1")));
    }

    @Test public void testBoostingQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(boostingQuery().positive(termQuery("field1", "value1")).negative(termQuery("field1", "value2")).negativeBoost(0.2f)).query();
        assertThat(parsedQuery, instanceOf(BoostingQuery.class));
    }

    @Test public void testBoostingQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/boosting-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(BoostingQuery.class));
    }

    @Test public void testBoolQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
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


    @Test public void testBoolQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/bool.json");
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

    @Test public void testTermsQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/terms-query.json");
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

    @Test public void testFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), termFilter("name.last", "banon"))).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFilteredQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/filtered-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFilteredQuery2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/filtered-query2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFilteredQuery3() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/filtered-query3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));

        Filter filter = filteredQuery.getFilter();
        assertThat(filter, instanceOf(NumericRangeFilter.class));
        NumericRangeFilter rangeFilter = (NumericRangeFilter) filter;
        assertThat(rangeFilter.getField(), equalTo("age"));
        assertThat(rangeFilter.getMin().intValue(), equalTo(23));
        assertThat(rangeFilter.getMax().intValue(), equalTo(54));
    }

    @Test public void testFilteredQuery4() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/filtered-query4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        WildcardQuery wildcardQuery = (WildcardQuery) filteredQuery.getQuery();
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
        assertThat((double) wildcardQuery.getBoost(), closeTo(1.1, 0.001));

        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testTermFilterQuery() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/term-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(TermFilter.class));
        TermFilter termFilter = (TermFilter) filteredQuery.getFilter();
        assertThat(termFilter.getTerm().field(), equalTo("name.last"));
        assertThat(termFilter.getTerm().text(), equalTo("banon"));
    }

    @Test public void testTermNamedFilterQuery() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/term-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery.query();
        assertThat(filteredQuery.getFilter(), instanceOf(TermFilter.class));
        TermFilter termFilter = (TermFilter) filteredQuery.getFilter();
        assertThat(termFilter.getTerm().field(), equalTo("name.last"));
        assertThat(termFilter.getTerm().text(), equalTo("banon"));
    }

    @Test public void testTermsFilterQueryBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), termsFilter("name.last", "banon", "kimchy"))).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(PublicTermsFilter.class));
        PublicTermsFilter termsFilter = (PublicTermsFilter) filteredQuery.getFilter();
        assertThat(termsFilter.getTerms().size(), equalTo(2));
        assertThat(termsFilter.getTerms().iterator().next().text(), equalTo("banon"));
    }


    @Test public void testTermsFilterQuery() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/terms-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(PublicTermsFilter.class));
        PublicTermsFilter termsFilter = (PublicTermsFilter) filteredQuery.getFilter();
        assertThat(termsFilter.getTerms().size(), equalTo(2));
        assertThat(termsFilter.getTerms().iterator().next().text(), equalTo("banon"));
    }

    @Test public void testTermsWithNameFilterQuery() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/terms-filter-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery.query();
        assertThat(filteredQuery.getFilter(), instanceOf(PublicTermsFilter.class));
        PublicTermsFilter termsFilter = (PublicTermsFilter) filteredQuery.getFilter();
        assertThat(termsFilter.getTerms().size(), equalTo(2));
        assertThat(termsFilter.getTerms().iterator().next().text(), equalTo("banon"));
    }

    @Test public void testConstantScoreQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(constantScoreQuery(termFilter("name.last", "banon"))).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        assertThat(((TermFilter) constantScoreQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testConstantScoreQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/constantScore-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        assertThat(((TermFilter) constantScoreQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    // Disabled since we need a current context to execute it...
//    @Test public void testCustomScoreQuery1() throws IOException {
//        IndexQueryParser queryParser = queryParser();
//        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/custom_score1.json");
//        Query parsedQuery = queryParser.parse(query).query();
//        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
//        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
//        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term("name.last", "banon")));
//        assertThat(functionScoreQuery.getFunction(), instanceOf(CustomScoreQueryParser.ScriptScoreFunction.class));
//    }

    @Test public void testCustomBoostFactorQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(customBoostFactorQuery(termQuery("name.last", "banon")).boostFactor(1.3f)).query();
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term("name.last", "banon")));
        assertThat((double) ((BoostScoreFunction) functionScoreQuery.getFunction()).getBoost(), closeTo(1.3, 0.001));
    }


    @Test public void testCustomBoostFactorQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/custom-boost-factor-query.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term("name.last", "banon")));
        assertThat((double) ((BoostScoreFunction) functionScoreQuery.getFunction()).getBoost(), closeTo(1.3, 0.001));
    }

    @Test public void testSpanTermQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanTermQuery("age", 34)).query();
        assertThat(parsedQuery, instanceOf(SpanTermQuery.class));
        SpanTermQuery termQuery = (SpanTermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testSpanTermQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/spanTerm.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanTermQuery.class));
        SpanTermQuery termQuery = (SpanTermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testSpanNotQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanNotQuery().include(spanTermQuery("age", 34)).exclude(spanTermQuery("age", 35))).query();
        assertThat(parsedQuery, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanNotQuery.getInclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNotQuery.getExclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
    }

    @Test public void testSpanNotQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/spanNot.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanNotQuery.getInclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNotQuery.getExclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
    }

    @Test public void testSpanFirstQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanFirstQuery(spanTermQuery("age", 34), 12)).query();
        assertThat(parsedQuery, instanceOf(SpanFirstQuery.class));
        SpanFirstQuery spanFirstQuery = (SpanFirstQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanFirstQuery.getMatch()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(spanFirstQuery.getEnd(), equalTo(12));
    }

    @Test public void testSpanFirstQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/spanFirst.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanFirstQuery.class));
        SpanFirstQuery spanFirstQuery = (SpanFirstQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanFirstQuery.getMatch()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(spanFirstQuery.getEnd(), equalTo(12));
    }

    @Test public void testSpanNearQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanNearQuery().clause(spanTermQuery("age", 34)).clause(spanTermQuery("age", 35)).clause(spanTermQuery("age", 36)).slop(12).inOrder(false).collectPayloads(false)).query();
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }

    @Test public void testSpanNearQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/spanNear.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }

    @Test public void testSpanOrQueryBuilder() throws IOException {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(spanOrQuery().clause(spanTermQuery("age", 34)).clause(spanTermQuery("age", 35)).clause(spanTermQuery("age", 36))).query();
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
    }

    @Test public void testSpanOrQuery() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/spanOr.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
    }

    @Test public void testQueryFilterBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), queryFilter(termQuery("name.last", "banon")))).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testQueryFilter() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/query-filter.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFQueryFilter() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/fquery-filter.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery.query();
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testMoreLikeThisBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(moreLikeThisQuery("name.first", "name.last").likeText("something").minTermFreq(1).maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testMoreLikeThis() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/mlt.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getMoreLikeFields()[1], equalTo("name.last"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testFuzzyLikeThisBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyLikeThisQuery("name.first", "name.last").likeText("something").maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test public void testFuzzyLikeThis() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/fuzzyLikeThis.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test public void testFuzzyLikeFieldThisBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(fuzzyLikeThisFieldQuery("name.first").likeText("something").maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test public void testFuzzyLikeThisField() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/fuzzyLikeThisField.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(FuzzyLikeThisQuery.class));
//        FuzzyLikeThisQuery fuzzyLikeThisQuery = (FuzzyLikeThisQuery) parsedQuery;
    }

    @Test public void testMoreLikeThisFieldBuilder() throws Exception {
        IndexQueryParser queryParser = queryParser();
        Query parsedQuery = queryParser.parse(moreLikeThisFieldQuery("name.first").likeText("something").minTermFreq(1).maxQueryTerms(12)).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testMoreLikeThisField() throws Exception {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/mltField.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testGeoDistanceFilterNamed() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery.query();
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter1() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter3() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter4() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter5() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance5.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter6() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance6.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter7() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance7.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter8() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance8.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter9() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance9.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter10() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance10.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter11() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance11.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoDistanceFilter12() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_distance12.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoDistanceFilter filter = (GeoDistanceFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.lat(), closeTo(40, 0.00001));
        assertThat(filter.lon(), closeTo(-70, 0.00001));
        assertThat(filter.distance(), closeTo(12, 0.00001));
    }

    @Test public void testGeoBoundingBoxFilterNamed() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_boundingbox-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.query(), instanceOf(DeletionAwareConstantScoreQuery.class));
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery.query();
        GeoBoundingBoxFilter filter = (GeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat, closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon, closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat, closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon, closeTo(-80, 0.00001));
    }


    @Test public void testGeoBoundingBoxFilter1() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_boundingbox1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoBoundingBoxFilter filter = (GeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat, closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon, closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat, closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon, closeTo(-80, 0.00001));
    }

    @Test public void testGeoBoundingBoxFilter2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_boundingbox2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoBoundingBoxFilter filter = (GeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat, closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon, closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat, closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon, closeTo(-80, 0.00001));
    }

    @Test public void testGeoBoundingBoxFilter3() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_boundingbox3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoBoundingBoxFilter filter = (GeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat, closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon, closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat, closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon, closeTo(-80, 0.00001));
    }

    @Test public void testGeoBoundingBoxFilter4() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_boundingbox4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoBoundingBoxFilter filter = (GeoBoundingBoxFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.topLeft().lat, closeTo(40, 0.00001));
        assertThat(filter.topLeft().lon, closeTo(-70, 0.00001));
        assertThat(filter.bottomRight().lat, closeTo(30, 0.00001));
        assertThat(filter.bottomRight().lon, closeTo(-80, 0.00001));
    }

    @Test public void testGeoPolygonNamedFilter() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_polygon-named.json");
        ParsedQuery parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery.namedFilters().containsKey("test"), equalTo(true));
        assertThat(parsedQuery.query(), instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery.query();
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(3));
        assertThat(filter.points()[0].lat, closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon, closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat, closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon, closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat, closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon, closeTo(-90, 0.00001));
    }

    @Test public void testGeoPolygonFilter1() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_polygon1.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(3));
        assertThat(filter.points()[0].lat, closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon, closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat, closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon, closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat, closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon, closeTo(-90, 0.00001));
    }

    @Test public void testGeoPolygonFilter2() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_polygon2.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(3));
        assertThat(filter.points()[0].lat, closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon, closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat, closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon, closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat, closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon, closeTo(-90, 0.00001));
    }

    @Test public void testGeoPolygonFilter3() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_polygon3.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(3));
        assertThat(filter.points()[0].lat, closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon, closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat, closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon, closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat, closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon, closeTo(-90, 0.00001));
    }

    @Test public void testGeoPolygonFilter4() throws IOException {
        IndexQueryParser queryParser = queryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/xcontent/geo_polygon4.json");
        Query parsedQuery = queryParser.parse(query).query();
        assertThat(parsedQuery, instanceOf(DeletionAwareConstantScoreQuery.class));
        DeletionAwareConstantScoreQuery constantScoreQuery = (DeletionAwareConstantScoreQuery) parsedQuery;
        GeoPolygonFilter filter = (GeoPolygonFilter) constantScoreQuery.getFilter();
        assertThat(filter.fieldName(), equalTo("location"));
        assertThat(filter.points().length, equalTo(3));
        assertThat(filter.points()[0].lat, closeTo(40, 0.00001));
        assertThat(filter.points()[0].lon, closeTo(-70, 0.00001));
        assertThat(filter.points()[1].lat, closeTo(30, 0.00001));
        assertThat(filter.points()[1].lon, closeTo(-80, 0.00001));
        assertThat(filter.points()[2].lat, closeTo(20, 0.00001));
        assertThat(filter.points()[2].lon, closeTo(-90, 0.00001));
    }
}
