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

package org.elasticsearch.index.query.json;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.filter.none.NoneFilterCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParser;
import org.elasticsearch.util.lucene.search.MoreLikeThisQuery;
import org.elasticsearch.util.lucene.search.Queries;
import org.elasticsearch.util.lucene.search.TermFilter;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.json.JsonFilterBuilders.*;
import static org.elasticsearch.index.query.json.JsonQueryBuilders.*;
import static org.elasticsearch.util.io.Streams.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
@Test
public class SimpleJsonIndexQueryParserTests {

    private final Index index = new Index("test");

    @Test public void testQueryStringBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(queryString("test").defaultField("content").phraseSlop(1));

        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        assertThat(termQuery.getTerm(), equalTo(new Term("content", "test")));
    }

    @Test public void testQueryString() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/query.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        assertThat(termQuery.getTerm(), equalTo(new Term("content", "test")));
    }

    @Test public void testQueryStringFields1Builder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content").field("name").useDisMax(false));
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(((TermQuery) bQuery.clauses().get(0).getQuery()).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) bQuery.clauses().get(1).getQuery()).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields1() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/query-fields1.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(((TermQuery) bQuery.clauses().get(0).getQuery()).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) bQuery.clauses().get(1).getQuery()).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields2Builder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content").field("name").useDisMax(true));
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields2() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/query-fields2.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
    }

    @Test public void testQueryStringFields3Builder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(queryString("test").field("content", 2.2f).field("name").useDisMax(true));
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat((double) disjuncts.get(0).getBoost(), closeTo(2.2, 0.01));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
        assertThat((double) disjuncts.get(1).getBoost(), closeTo(1, 0.01));
    }

    @Test public void testQueryStringFields3() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/query-fields3.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        List<Query> disjuncts = Queries.disMaxClauses(disMaxQuery);
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term("content", "test")));
        assertThat((double) disjuncts.get(0).getBoost(), closeTo(2.2, 0.01));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term("name", "test")));
        assertThat((double) disjuncts.get(1).getBoost(), closeTo(1, 0.01));
    }

    @Test public void testMatchAllBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(matchAllQuery().boost(1.2f).buildAsString());
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
        MatchAllDocsQuery matchAllDocsQuery = (MatchAllDocsQuery) parsedQuery;
        assertThat((double) matchAllDocsQuery.getBoost(), closeTo(1.2, 0.01));
    }

    @Test public void testMatchAll() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/matchAll.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(MatchAllDocsQuery.class));
        MatchAllDocsQuery matchAllDocsQuery = (MatchAllDocsQuery) parsedQuery;
        assertThat((double) matchAllDocsQuery.getBoost(), closeTo(1.2, 0.01));
    }

    @Test public void testDisMaxBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(disMaxQuery().boost(1.2f).tieBreakerMultiplier(0.7f).add(termQuery("age", 34)).add(termQuery("age", 35)));
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        assertThat((double) disjunctionMaxQuery.getBoost(), closeTo(1.2, 0.01));

        List<Query> disjuncts = Queries.disMaxClauses(disjunctionMaxQuery);
        assertThat(disjuncts.size(), equalTo(2));

        Query firstQ = disjuncts.get(0);
        assertThat(firstQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) firstQ).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));

        Query secondsQ = disjuncts.get(1);
        assertThat(secondsQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) secondsQ).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
    }

    @Test public void testDisMax() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/disMax.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) parsedQuery;
        assertThat((double) disjunctionMaxQuery.getBoost(), closeTo(1.2, 0.01));

        List<Query> disjuncts = Queries.disMaxClauses(disjunctionMaxQuery);
        assertThat(disjuncts.size(), equalTo(2));

        Query firstQ = disjuncts.get(0);
        assertThat(firstQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) firstQ).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));

        Query secondsQ = disjuncts.get(1);
        assertThat(secondsQ, instanceOf(TermQuery.class));
        assertThat(((TermQuery) secondsQ).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
    }

    @Test public void testTermQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(termQuery("age", 34).buildAsBytes());
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testTermQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/term.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testFieldQueryBuilder1() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(fieldQuery("age", 34).buildAsBytes());
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testFieldQuery1() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/field1.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testFieldQuery2() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/field2.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) parsedQuery;
        assertThat(bQuery.getClauses().length, equalTo(2));
        assertThat(((TermQuery) bQuery.getClauses()[0].getQuery()).getTerm().field(), equalTo("name.first"));
        assertThat(((TermQuery) bQuery.getClauses()[0].getQuery()).getTerm().text(), equalTo("12-54-23"));
        assertThat(((TermQuery) bQuery.getClauses()[1].getQuery()).getTerm().field(), equalTo("name.first"));
        assertThat(((TermQuery) bQuery.getClauses()[1].getQuery()).getTerm().text(), equalTo("else"));
    }

    @Test public void testFieldQuery3() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/field3.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat((double) parsedQuery.getBoost(), closeTo(2.0, 0.01));
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testTermWithBoostQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(termQuery("age", 34).boost(2.0f));
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat((double) termQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testTermWithBoostQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/term-with-boost.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat((double) termQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testPrefixQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("name.first", "sh"));
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/prefix.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), prefixFilter("name.first", "sh")));
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixFilteredQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/prefix-filter.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        PrefixFilter prefixFilter = (PrefixFilter) filteredQuery.getFilter();
        assertThat(prefixFilter.getPrefix(), equalTo(new Term("name.first", "sh")));
    }

    @Test public void testPrefixQueryBoostQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(prefixQuery("name.first", "sh").boost(2.0f));
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testPrefixQueryBoostQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/prefix-with-boost.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(PrefixQuery.class));
        PrefixQuery prefixQuery = (PrefixQuery) parsedQuery;
        assertThat(prefixQuery.getPrefix(), equalTo(new Term("name.first", "sh")));
        assertThat((double) prefixQuery.getBoost(), closeTo(2.0, 0.01));
    }

    @Test public void testWildcardQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(wildcardQuery("name.first", "sh*"));
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
    }

    @Test public void testWildcardQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/wildcard.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(WildcardQuery.class));
        WildcardQuery wildcardQuery = (WildcardQuery) parsedQuery;
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
    }

    @Test public void testRangeQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(rangeQuery("age").from(23).to(54).includeLower(true).includeUpper(false));
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
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/range.json");
        Query parsedQuery = queryParser.parse(query);
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
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), rangeFilter("age").from(23).to(54).includeLower(true).includeUpper(false)));
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
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/range-filter.json");
        Query parsedQuery = queryParser.parse(query);
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

    @Test public void testBoolFilteredQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/bool-filter.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        BooleanFilter booleanFilter = (BooleanFilter) filteredQuery.getFilter();

        // TODO get the content and test
    }

    @Test public void testBoolQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(boolQuery().must(termQuery("content", "test1")).must(termQuery("content", "test4")).mustNot(termQuery("content", "test2")).should(termQuery("content", "test3")));
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
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/bool.json");
        Query parsedQuery = queryParser.parse(query);
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

    @Test public void testFilteredQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), termFilter("name.last", "banon")));
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFilteredQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/filtered-query.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFilteredQuery2() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/filtered-query2.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(((TermQuery) filteredQuery.getQuery()).getTerm(), equalTo(new Term("name.first", "shay")));
        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testFilteredQuery3() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/filtered-query3.json");
        Query parsedQuery = queryParser.parse(query);
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
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/filtered-query4.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        WildcardQuery wildcardQuery = (WildcardQuery) filteredQuery.getQuery();
        assertThat(wildcardQuery.getTerm(), equalTo(new Term("name.first", "sh*")));
        assertThat((double) wildcardQuery.getBoost(), closeTo(1.1, 0.001));

        assertThat(((TermFilter) filteredQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testTermsFilterQueryBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), termsFilter("name.last", "banon", "kimchy")));
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(TermsFilter.class));
        TermsFilter termsFilter = (TermsFilter) filteredQuery.getFilter();
        Field field = TermsFilter.class.getDeclaredField("terms");
        field.setAccessible(true);
        Set<Term> terms = (Set<Term>) field.get(termsFilter);
        assertThat(terms.size(), equalTo(2));
        assertThat(terms.iterator().next().text(), equalTo("banon"));
    }


    @Test public void testTermsFilterQuery() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/terms-filter.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        assertThat(filteredQuery.getFilter(), instanceOf(TermsFilter.class));
        TermsFilter termsFilter = (TermsFilter) filteredQuery.getFilter();
        Field field = TermsFilter.class.getDeclaredField("terms");
        field.setAccessible(true);
        Set<Term> terms = (Set<Term>) field.get(termsFilter);
        assertThat(terms.size(), equalTo(2));
        assertThat(terms.iterator().next().text(), equalTo("banon"));
    }

    @Test public void testConstantScoreQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(constantScoreQuery(termFilter("name.last", "banon")));
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) parsedQuery;
        assertThat(((TermFilter) constantScoreQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testConstantScoreQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/constantScore-query.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(ConstantScoreQuery.class));
        ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) parsedQuery;
        assertThat(((TermFilter) constantScoreQuery.getFilter()).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testSpanTermQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(spanTermQuery("age", 34));
        assertThat(parsedQuery, instanceOf(SpanTermQuery.class));
        SpanTermQuery termQuery = (SpanTermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testSpanTermQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/spanTerm.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(SpanTermQuery.class));
        SpanTermQuery termQuery = (SpanTermQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(termQuery.getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
    }

    @Test public void testSpanNotQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(spanNotQuery().include(spanTermQuery("age", 34)).exclude(spanTermQuery("age", 35)));
        assertThat(parsedQuery, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanNotQuery.getInclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNotQuery.getExclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
    }

    @Test public void testSpanNotQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/spanNot.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(SpanNotQuery.class));
        SpanNotQuery spanNotQuery = (SpanNotQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanNotQuery.getInclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNotQuery.getExclude()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
    }

    @Test public void testSpanFirstQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(spanFirstQuery(spanTermQuery("age", 34), 12));
        assertThat(parsedQuery, instanceOf(SpanFirstQuery.class));
        SpanFirstQuery spanFirstQuery = (SpanFirstQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanFirstQuery.getMatch()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(spanFirstQuery.getEnd(), equalTo(12));
    }

    @Test public void testSpanFirstQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/spanFirst.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(SpanFirstQuery.class));
        SpanFirstQuery spanFirstQuery = (SpanFirstQuery) parsedQuery;
        // since age is automatically registered in data, we encode it as numeric
        assertThat(((SpanTermQuery) spanFirstQuery.getMatch()).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(spanFirstQuery.getEnd(), equalTo(12));
    }

    @Test public void testSpanNearQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(spanNearQuery().clause(spanTermQuery("age", 34)).clause(spanTermQuery("age", 35)).clause(spanTermQuery("age", 36)).slop(12).inOrder(false).collectPayloads(false));
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }

    @Test public void testSpanNearQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/spanNear.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) parsedQuery;
        assertThat(spanNearQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanNearQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
        assertThat(spanNearQuery.isInOrder(), equalTo(false));
    }

    @Test public void testSpanOrQueryBuilder() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(spanOrQuery().clause(spanTermQuery("age", 34)).clause(spanTermQuery("age", 35)).clause(spanTermQuery("age", 36)));
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
    }

    @Test public void testSpanOrQuery() throws IOException {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/spanOr.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) parsedQuery;
        assertThat(spanOrQuery.getClauses().length, equalTo(3));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[0]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(34))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[1]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(35))));
        assertThat(((SpanTermQuery) spanOrQuery.getClauses()[2]).getTerm(), equalTo(new Term("age", NumericUtils.longToPrefixCoded(36))));
    }

    @Test public void testQueryFilterBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(filteredQuery(termQuery("name.first", "shay"), queryFilter(termQuery("name.last", "banon"))));
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
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/query-filter.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(FilteredQuery.class));
        FilteredQuery filteredQuery = (FilteredQuery) parsedQuery;
        QueryWrapperFilter queryWrapperFilter = (QueryWrapperFilter) filteredQuery.getFilter();
        Field field = QueryWrapperFilter.class.getDeclaredField("query");
        field.setAccessible(true);
        Query wrappedQuery = (Query) field.get(queryWrapperFilter);
        assertThat(wrappedQuery, instanceOf(TermQuery.class));
        assertThat(((TermQuery) wrappedQuery).getTerm(), equalTo(new Term("name.last", "banon")));
    }

    @Test public void testMoreLikeThisBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(moreLikeThisQuery("name.first", "name.last").likeText("something").minTermFrequency(1).maxQueryTerms(12));
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testMoreLikeThis() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/mlt.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getMoreLikeFields()[1], equalTo("name.last"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testMoreLikeThisFieldBuilder() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        Query parsedQuery = queryParser.parse(moreLikeThisFieldQuery("name.first").likeText("something").minTermFrequency(1).maxQueryTerms(12));
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    @Test public void testMoreLikeThisField() throws Exception {
        IndexQueryParser queryParser = newQueryParser();
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/json/mltField.json");
        Query parsedQuery = queryParser.parse(query);
        assertThat(parsedQuery, instanceOf(MoreLikeThisQuery.class));
        MoreLikeThisQuery mltQuery = (MoreLikeThisQuery) parsedQuery;
        assertThat(mltQuery.getMoreLikeFields()[0], equalTo("name.first"));
        assertThat(mltQuery.getLikeText(), equalTo("something"));
        assertThat(mltQuery.getMinTermFrequency(), equalTo(1));
        assertThat(mltQuery.getMaxQueryTerms(), equalTo(12));
    }

    private JsonIndexQueryParser newQueryParser() throws IOException {
        return new JsonIndexQueryParser(new Index("test"), EMPTY_SETTINGS,
                newMapperService(), new NoneFilterCache(index, EMPTY_SETTINGS), new AnalysisService(index), null, null, "test", null);
    }

    private MapperService newMapperService() throws IOException {
        Environment environment = new Environment();
        MapperService mapperService = new MapperService(index, EMPTY_SETTINGS, environment, new AnalysisService(index));
        // init a mapping with data
        mapperService.type("person").parse(copyToBytesFromClasspath("/org/elasticsearch/index/query/json/data.json"));
        return mapperService;
    }
}
