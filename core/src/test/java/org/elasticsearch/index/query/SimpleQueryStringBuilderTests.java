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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimpleQueryStringBuilderTests extends AbstractQueryTestCase<SimpleQueryStringBuilder> {

    @Override
    protected SimpleQueryStringBuilder doCreateTestQueryBuilder() {
        SimpleQueryStringBuilder result = new SimpleQueryStringBuilder(randomAsciiOfLengthBetween(1, 10));
        if (randomBoolean()) {
            result.analyzeWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            result.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            result.analyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            result.defaultOperator(randomFrom(Operator.values()));
        }
        if (randomBoolean()) {
            result.quoteFieldSuffix(TestUtil.randomSimpleString(random()));
        }
        if (randomBoolean()) {
            Set<SimpleQueryStringFlag> flagSet = new HashSet<>();
            int size = randomIntBetween(0, SimpleQueryStringFlag.values().length);
            for (int i = 0; i < size; i++) {
                flagSet.add(randomFrom(SimpleQueryStringFlag.values()));
            }
            if (flagSet.size() > 0) {
                result.flags(flagSet.toArray(new SimpleQueryStringFlag[flagSet.size()]));
            }
        }

        int fieldCount = randomIntBetween(0, 10);
        Map<String, Float> fields = new HashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            if (randomBoolean()) {
                fields.put(randomAsciiOfLengthBetween(1, 10), AbstractQueryBuilder.DEFAULT_BOOST);
            } else {
                fields.put(randomBoolean() ? STRING_FIELD_NAME : randomAsciiOfLengthBetween(1, 10), 2.0f / randomIntBetween(1, 20));
            }
        }
        result.fields(fields);

        return result;
    }

    public void testDefaults() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");

        assertEquals("Wrong default default boost.", AbstractQueryBuilder.DEFAULT_BOOST, qb.boost(), 0.001);
        assertEquals("Wrong default default boost field.", AbstractQueryBuilder.DEFAULT_BOOST, SimpleQueryStringBuilder.DEFAULT_BOOST,
                0.001);

        assertEquals("Wrong default flags.", SimpleQueryStringFlag.ALL.value, qb.flags());
        assertEquals("Wrong default flags field.", SimpleQueryStringFlag.ALL.value(), SimpleQueryStringBuilder.DEFAULT_FLAGS);

        assertEquals("Wrong default default operator.", Operator.OR, qb.defaultOperator());
        assertEquals("Wrong default default operator field.", Operator.OR, SimpleQueryStringBuilder.DEFAULT_OPERATOR);

        assertEquals("Wrong default default analyze_wildcard.", false, qb.analyzeWildcard());
        assertEquals("Wrong default default analyze_wildcard field.", false, SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD);

        assertEquals("Wrong default default lenient.", false, qb.lenient());
        assertEquals("Wrong default default lenient field.", false, SimpleQueryStringBuilder.DEFAULT_LENIENT);
    }

    public void testDefaultNullComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags((SimpleQueryStringFlag[]) null);
        assertEquals("Setting flags to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_FLAGS,
                qb.flags());
    }

    public void testDefaultEmptyComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags(new SimpleQueryStringFlag[]{});
        assertEquals("Setting flags to empty should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_FLAGS,
                qb.flags());
    }

    public void testDefaultNullComplainOp() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.defaultOperator(null);
        assertEquals("Setting operator to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_OPERATOR,
                qb.defaultOperator());
    }

    // Check operator handling, and default field handling.
    public void testDefaultOperatorHandling() throws IOException {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.").field(STRING_FIELD_NAME);
        QueryShardContext shardContext = createShardContext();
        shardContext.setAllowUnmappedFields(true); // to avoid occasional cases
                                                   // in setup where we didn't
                                                   // add types but strict field
                                                   // resolution
        BooleanQuery boolQuery = (BooleanQuery) qb.toQuery(shardContext);
        assertThat(shouldClauses(boolQuery), is(4));

        qb.defaultOperator(Operator.AND);
        boolQuery = (BooleanQuery) qb.toQuery(shardContext);
        assertThat(shouldClauses(boolQuery), is(0));

        qb.defaultOperator(Operator.OR);
        boolQuery = (BooleanQuery) qb.toQuery(shardContext);
        assertThat(shouldClauses(boolQuery), is(4));
    }

    public void testIllegalConstructorArg() {
        expectThrows(IllegalArgumentException.class, () -> new SimpleQueryStringBuilder((String) null));
    }

    public void testFieldCannotBeNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.field(null));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    public void testFieldCannotBeNullAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.field(null, AbstractQueryBuilder.DEFAULT_BOOST));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    public void testFieldCannotBeEmpty() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.field(""));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    public void testFieldCannotBeEmptyAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException  e = expectThrows(IllegalArgumentException.class, () -> qb.field("", AbstractQueryBuilder.DEFAULT_BOOST));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    /**
     * The following should fail fast - never silently set the map containing
     * fields and weights to null but refuse to accept null instead.
     * */
    public void testFieldsCannotBeSetToNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        NullPointerException e = expectThrows(NullPointerException.class, () -> qb.fields(null));
        assertEquals("fields cannot be null", e.getMessage());
    }

    public void testDefaultFieldParsing() throws IOException {
        String query = randomAsciiOfLengthBetween(1, 10).toLowerCase(Locale.ROOT);
        String contentString = "{\n" +
                "    \"simple_query_string\" : {\n" +
                "      \"query\" : \"" + query + "\"" +
                "    }\n" +
                "}";
        SimpleQueryStringBuilder queryBuilder = (SimpleQueryStringBuilder) parseQuery(contentString);
        assertThat(queryBuilder.value(), equalTo(query));
        assertThat(queryBuilder.fields(), notNullValue());
        assertThat(queryBuilder.fields().size(), equalTo(0));
        QueryShardContext shardContext = createShardContext();

        // the remaining tests requires either a mapping that we register with types in base test setup
        if (getCurrentTypes().length > 0) {
            Query luceneQuery = queryBuilder.toQuery(shardContext);
            assertThat(luceneQuery, instanceOf(BooleanQuery.class));
        }
    }

    /*
     * This assumes that Lucene query parsing is being checked already, adding
     * checks only for our parsing extensions.
     *
     * Also this relies on {@link SimpleQueryStringTests} to test most of the
     * actual functionality of query parsing.
     */
    @Override
    protected void doAssertLuceneQuery(SimpleQueryStringBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, notNullValue());

        if ("".equals(queryBuilder.value())) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else if (queryBuilder.fields().size() > 1) {
            assertThat(query, instanceOf(BooleanQuery.class));
            BooleanQuery boolQuery = (BooleanQuery) query;
            for (BooleanClause clause : boolQuery.clauses()) {
                if (clause.getQuery() instanceof TermQuery) {
                    TermQuery inner = (TermQuery) clause.getQuery();
                    assertThat(inner.getTerm().bytes().toString(), is(inner.getTerm().bytes().toString().toLowerCase(Locale.ROOT)));
                }
            }
            assertThat(boolQuery.clauses().size(), equalTo(queryBuilder.fields().size()));
            Iterator<Map.Entry<String, Float>> fieldsIterator = queryBuilder.fields().entrySet().iterator();
            for (BooleanClause booleanClause : boolQuery) {
                Map.Entry<String, Float> field = fieldsIterator.next();
                assertTermOrBoostQuery(booleanClause.getQuery(), field.getKey(), queryBuilder.value(), field.getValue());
            }
            if (queryBuilder.minimumShouldMatch() != null && !boolQuery.isCoordDisabled()) {
                assertThat(boolQuery.getMinimumNumberShouldMatch(), greaterThan(0));
            }
        } else if (queryBuilder.fields().size() == 1) {
            Map.Entry<String, Float> field = queryBuilder.fields().entrySet().iterator().next();
            assertTermOrBoostQuery(query, field.getKey(), queryBuilder.value(), field.getValue());
        } else if (queryBuilder.fields().size() == 0) {
            MapperService ms = context.mapperService();
            if (ms.allEnabled()) {
                assertTermQuery(query, MetaData.ALL, queryBuilder.value());
            } else {
                assertThat(query.getClass(), anyOf(equalTo(BooleanQuery.class), equalTo(MatchNoDocsQuery.class)));
            }
        } else {
            fail("Encountered lucene query type we do not have a validation implementation for in our "
                    + SimpleQueryStringBuilderTests.class.getSimpleName());
        }
    }

    private static int shouldClauses(BooleanQuery query) {
        int result = 0;
        for (BooleanClause c : query.clauses()) {
            if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                result++;
            }
        }
        return result;
    }

    public void testToQueryBoost() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryShardContext shardContext = createShardContext();
        SimpleQueryStringBuilder simpleQueryStringBuilder = new SimpleQueryStringBuilder("test");
        simpleQueryStringBuilder.field(STRING_FIELD_NAME, 5);
        Query query = simpleQueryStringBuilder.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(5f));
        assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));

        simpleQueryStringBuilder = new SimpleQueryStringBuilder("test");
        simpleQueryStringBuilder.field(STRING_FIELD_NAME, 5);
        simpleQueryStringBuilder.boost(2);
        query = simpleQueryStringBuilder.toQuery(shardContext);
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2f));
        assertThat(boostQuery.getQuery(), instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) boostQuery.getQuery();
        assertThat(boostQuery.getBoost(), equalTo(5f));
        assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));
    }

    public void testNegativeFlags() throws IOException {
        String query = "{\"simple_query_string\": {\"query\": \"foo bar\", \"flags\": -1}}";
        SimpleQueryStringBuilder builder = new SimpleQueryStringBuilder("foo bar");
        builder.flags(SimpleQueryStringFlag.ALL);
        assertParsedQuery(query, builder);
        SimpleQueryStringBuilder otherBuilder = new SimpleQueryStringBuilder("foo bar");
        otherBuilder.flags(-1);
        assertThat(builder, equalTo(otherBuilder));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"simple_query_string\" : {\n" +
                "    \"query\" : \"\\\"fried eggs\\\" +(eggplant | potato) -frittata\",\n" +
                "    \"fields\" : [ \"_all^1.0\", \"body^5.0\" ],\n" +
                "    \"analyzer\" : \"snowball\",\n" +
                "    \"flags\" : -1,\n" +
                "    \"default_operator\" : \"and\",\n" +
                "    \"lenient\" : false,\n" +
                "    \"analyze_wildcard\" : false,\n" +
                "    \"quote_field_suffix\" : \".quote\",\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        SimpleQueryStringBuilder parsed = (SimpleQueryStringBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "\"fried eggs\" +(eggplant | potato) -frittata", parsed.value());
        assertEquals(json, 2, parsed.fields().size());
        assertEquals(json, "snowball", parsed.analyzer());
        assertEquals(json, ".quote", parsed.quoteFieldSuffix());
    }

    public void testMinimumShouldMatch() throws IOException {
        QueryShardContext shardContext = createShardContext();
        int numberOfTerms = randomIntBetween(1, 4);
        StringBuilder queryString = new StringBuilder();
        for (int i = 0; i < numberOfTerms; i++) {
            queryString.append("t" + i + " ");
        }
        SimpleQueryStringBuilder simpleQueryStringBuilder = new SimpleQueryStringBuilder(queryString.toString().trim());
        if (randomBoolean()) {
            simpleQueryStringBuilder.defaultOperator(Operator.AND);
        }
        int numberOfFields = randomIntBetween(1, 4);
        for (int i = 0; i < numberOfFields; i++) {
            simpleQueryStringBuilder.field("f" + i);
        }
        int percent = randomIntBetween(1, 100);
        simpleQueryStringBuilder.minimumShouldMatch(percent + "%");
        Query query = simpleQueryStringBuilder.toQuery(shardContext);

        // check special case: one term & one field should get simplified to a TermQuery
        if (numberOfFields * numberOfTerms == 1) {
            assertThat(query, instanceOf(TermQuery.class));
        } else {
            assertThat(query, instanceOf(BooleanQuery.class));
            BooleanQuery boolQuery = (BooleanQuery) query;
            int expectedMinimumShouldMatch = numberOfTerms * percent / 100;
            if (numberOfTerms == 1 || simpleQueryStringBuilder.defaultOperator().equals(Operator.AND)) {
                expectedMinimumShouldMatch = 0;
            }
            assertEquals(expectedMinimumShouldMatch, boolQuery.getMinimumNumberShouldMatch());
        }
    }

    public void testIndexMetaField() throws IOException {
        QueryShardContext shardContext = createShardContext();
        SimpleQueryStringBuilder simpleQueryStringBuilder = new SimpleQueryStringBuilder(getIndex().getName());
        simpleQueryStringBuilder.field("_index");
        Query query = simpleQueryStringBuilder.toQuery(shardContext);
        assertThat(query, notNullValue());
        if (getCurrentTypes().length > 0) {
            assertThat(query, instanceOf(MatchAllDocsQuery.class));
        }
    }

    public void testExpandedTerms() throws Exception {
        // Prefix
        Query query = new SimpleQueryStringBuilder("aBc*")
                .field(STRING_FIELD_NAME)
                .analyzer("whitespace")
                .toQuery(createShardContext());
        assertEquals(new PrefixQuery(new Term(STRING_FIELD_NAME, "aBc")), query);
        query = new SimpleQueryStringBuilder("aBc*")
                .field(STRING_FIELD_NAME)
                .analyzer("standard")
                .toQuery(createShardContext());
        assertEquals(new PrefixQuery(new Term(STRING_FIELD_NAME, "abc")), query);

        // Fuzzy
        query = new SimpleQueryStringBuilder("aBc~1")
                .field(STRING_FIELD_NAME)
                .analyzer("whitespace")
                .toQuery(createShardContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(STRING_FIELD_NAME, "aBc"), 1);
        assertEquals(expected, query);
        query = new SimpleQueryStringBuilder("aBc~1")
                .field(STRING_FIELD_NAME)
                .analyzer("standard")
                .toQuery(createShardContext());
        expected = new FuzzyQuery(new Term(STRING_FIELD_NAME, "abc"), 1);
        assertEquals(expected, query);
    }

    public void testAllFieldsWithFields() throws IOException {
        String json =
                "{\n" +
                "  \"simple_query_string\" : {\n" +
                "    \"query\" : \"this that thus\",\n" +
                "    \"fields\" : [\"foo\"],\n" +
                "    \"all_fields\" : true\n" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(),
                containsString("cannot use [all_fields] parameter in conjunction with [fields]"));
    }
}
