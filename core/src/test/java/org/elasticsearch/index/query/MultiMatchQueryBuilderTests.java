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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.search.MatchQuery;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBooleanSubQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class MultiMatchQueryBuilderTests extends AbstractQueryTestCase<MultiMatchQueryBuilder> {

    @Override
    protected MultiMatchQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME);
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder(getRandomValueForFieldName(fieldName), fieldName);
        // field with random boost
        if (randomBoolean()) {
            query.field(fieldName, randomFloat() * 10);
        }
        // field with a pattern
        if (randomBoolean()) {
            query.field('*' + fieldName);
        }
        // sets other parameters of the multi match query
        if (randomBoolean()) {
            query.type(randomFrom(MultiMatchQueryBuilder.Type.values()));
        }
        if (randomBoolean()) {
            query.operator(randomFrom(Operator.values()));
        }
        if (randomBoolean()) {
            query.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }
        if (randomBoolean()) {
            query.slop(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            query.fuzziness(randomFuzziness(fieldName));
        }
        if (randomBoolean()) {
            query.prefixLength(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            query.maxExpansions(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            query.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            query.fuzzyRewrite(getRandomRewriteMethod());
        }
        if (randomBoolean()) {
            query.useDisMax(randomBoolean());
        }
        if (randomBoolean()) {
            query.tieBreaker(randomFloat() * 10);
        }
        if (randomBoolean()) {
            query.lenient(randomBoolean());
        }
        if (randomBoolean()) {
            query.cutoffFrequency((float) 10 / randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            query.zeroTermsQuery(randomFrom(MatchQuery.ZeroTermsQuery.values()));
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(MultiMatchQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {

    }

    @Test
    public void testValidate() {
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(null, "field");
        assertThat(multiMatchQueryBuilder.validate().validationErrors().size(), is(1));

        multiMatchQueryBuilder = new MultiMatchQueryBuilder("text");
        assertThat(multiMatchQueryBuilder.validate().validationErrors().size(), is(1));

        multiMatchQueryBuilder = new MultiMatchQueryBuilder(null);
        assertThat(multiMatchQueryBuilder.validate().validationErrors().size(), is(2));

        multiMatchQueryBuilder = new MultiMatchQueryBuilder("text", "field");
        assertNull(multiMatchQueryBuilder.validate());
    }

    @Override
    protected void assertBoost(MultiMatchQueryBuilder queryBuilder, Query query) throws IOException {
        //we delegate boost checks to specific boost tests below
    }

    @Test
    public void testToQueryBoost() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryShardContext shardContext = createShardContext();
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("test");
        multiMatchQueryBuilder.field(STRING_FIELD_NAME, 5);
        Query query = multiMatchQueryBuilder.toQuery(shardContext);
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(query.getBoost(), equalTo(5f));

        multiMatchQueryBuilder = new MultiMatchQueryBuilder("test");
        multiMatchQueryBuilder.field(STRING_FIELD_NAME, 5);
        multiMatchQueryBuilder.boost(2);
        query = multiMatchQueryBuilder.toQuery(shardContext);
        assertThat(query, instanceOf(TermQuery.class));
        assertThat(query.getBoost(), equalTo(10f));
    }

    @Test
    public void testToQueryMultipleTermsBooleanQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test1 test2").field(STRING_FIELD_NAME).useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test1")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test2")));
    }

    @Test
    public void testToQueryMultipleFieldsBooleanQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2).useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    @Test
    public void testToQueryMultipleFieldsDisMaxQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2).useDisMax(true).toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) query;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    @Test
    public void testToQueryFieldsWildcard() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test").field("mapped_str*").useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }
}
