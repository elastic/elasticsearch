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
import org.apache.lucene.search.*;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.*;

public class SimpleQueryStringBuilderTests extends BaseQueryTestCase<SimpleQueryStringBuilder> {

    private static final String[] MINIMUM_SHOULD_MATCH = new String[] { "1", "-1", "75%", "-25%", "2<75%", "2<-25%" };

    @Override
    protected SimpleQueryStringBuilder doCreateTestQueryBuilder() {
        SimpleQueryStringBuilder result = new SimpleQueryStringBuilder(randomAsciiOfLengthBetween(1, 10));
        if (randomBoolean()) {
            result.analyzeWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            result.lenient(randomBoolean());
        }
        if (randomBoolean()) {
            result.lowercaseExpandedTerms(randomBoolean());
        }
        if (randomBoolean()) {
            result.locale(randomLocale(getRandom()));
        }
        if (randomBoolean()) {
            result.minimumShouldMatch(randomFrom(MINIMUM_SHOULD_MATCH));
        }
        if (randomBoolean()) {
            result.analyzer("simple");
        }
        if (randomBoolean()) {
            result.defaultOperator(randomFrom(Operator.AND, Operator.OR));
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
        Map<String, Float> fields = new TreeMap<>();
        for (int i = 0; i < fieldCount; i++) {
            if (randomBoolean()) {
                fields.put(randomAsciiOfLengthBetween(1, 10), AbstractQueryBuilder.DEFAULT_BOOST);
            } else {
                fields.put(randomAsciiOfLengthBetween(1, 10), 2.0f / randomIntBetween(1, 20));
            }
        }
        result.fields(fields);

        return result;
    }

    @Test
    public void testDefaults() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");

        assertEquals("Wrong default default boost.", AbstractQueryBuilder.DEFAULT_BOOST, qb.boost(), 0.001);
        assertEquals("Wrong default default boost field.", AbstractQueryBuilder.DEFAULT_BOOST, SimpleQueryStringBuilder.DEFAULT_BOOST,
                0.001);

        assertEquals("Wrong default flags.", SimpleQueryStringFlag.ALL.value, qb.flags());
        assertEquals("Wrong default flags field.", SimpleQueryStringFlag.ALL.value(), SimpleQueryStringBuilder.DEFAULT_FLAGS);

        assertEquals("Wrong default default operator.", Operator.OR, qb.defaultOperator());
        assertEquals("Wrong default default operator field.", Operator.OR, SimpleQueryStringBuilder.DEFAULT_OPERATOR);

        assertEquals("Wrong default default locale.", Locale.ROOT, qb.locale());
        assertEquals("Wrong default default locale field.", Locale.ROOT, SimpleQueryStringBuilder.DEFAULT_LOCALE);

        assertEquals("Wrong default default analyze_wildcard.", false, qb.analyzeWildcard());
        assertEquals("Wrong default default analyze_wildcard field.", false, SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD);

        assertEquals("Wrong default default lowercase_expanded_terms.", true, qb.lowercaseExpandedTerms());
        assertEquals("Wrong default default lowercase_expanded_terms field.", true,
                SimpleQueryStringBuilder.DEFAULT_LOWERCASE_EXPANDED_TERMS);

        assertEquals("Wrong default default lenient.", false, qb.lenient());
        assertEquals("Wrong default default lenient field.", false, SimpleQueryStringBuilder.DEFAULT_LENIENT);

        assertEquals("Wrong default default locale.", Locale.ROOT, qb.locale());
        assertEquals("Wrong default default locale field.", Locale.ROOT, SimpleQueryStringBuilder.DEFAULT_LOCALE);
    }

    @Test
    public void testDefaultNullLocale() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.locale(null);
        assertEquals("Setting locale to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_LOCALE,
                qb.locale());
    }

    @Test
    public void testDefaultNullComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags((SimpleQueryStringFlag[]) null);
        assertEquals("Setting flags to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_FLAGS,
                qb.flags());
    }

    @Test
    public void testDefaultEmptyComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags(new SimpleQueryStringFlag[] {});
        assertEquals("Setting flags to empty should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_FLAGS,
                qb.flags());
    }

    @Test
    public void testDefaultNullComplainOp() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.defaultOperator(null);
        assertEquals("Setting operator to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_OPERATOR,
                qb.defaultOperator());
    }

    // Check operator handling, and default field handling.
    @Test
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

    @Test
    public void testValidation() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        assertNull(qb.validate());
    }

    @Test
    public void testNullQueryTextGeneratesException() {
        SimpleQueryStringBuilder builder = new SimpleQueryStringBuilder(null);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeNullAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field(null, AbstractQueryBuilder.DEFAULT_BOOST);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeEmpty() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeEmptyAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field("", AbstractQueryBuilder.DEFAULT_BOOST);
    }

    /**
     * The following should fail fast - never silently set the map containing
     * fields and weights to null but refuse to accept null instead.
     * */
    @Test(expected = NullPointerException.class)
    public void testFieldsCannotBeSetToNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.fields(null);
    }

    @Test
    public void testDefaultFieldParsing() throws IOException {
        QueryParseContext context = createParseContext();
        String query = randomAsciiOfLengthBetween(1, 10).toLowerCase(Locale.ROOT);
        String contentString = "{\n" +
                "    \"simple_query_string\" : {\n" +
                "      \"query\" : \"" + query + "\"" +
                "    }\n" +
                "}";
        XContentParser parser = XContentFactory.xContent(contentString).createParser(contentString);
        context.reset(parser);
        SimpleQueryStringBuilder queryBuilder = new SimpleQueryStringParser().fromXContent(context);
        assertThat(queryBuilder.value(), equalTo(query));
        assertThat(queryBuilder.fields(), notNullValue());
        assertThat(queryBuilder.fields().size(), equalTo(0));
        QueryShardContext shardContext = createShardContext();

        // the remaining tests requires either a mapping that we register with types in base test setup
        // no strict field resolution (version before V_1_4_0_Beta1)
        if (getCurrentTypes().length > 0 || shardContext.indexQueryParserService().getIndexCreatedVersion().before(Version.V_1_4_0_Beta1)) {
            Query luceneQuery = queryBuilder.toQuery(shardContext);
            assertThat(luceneQuery, instanceOf(TermQuery.class));
            TermQuery termQuery = (TermQuery) luceneQuery;
            assertThat(termQuery.getTerm(), equalTo(new Term(MetaData.ALL, query)));
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
    protected void doAssertLuceneQuery(SimpleQueryStringBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, notNullValue());

        if ("".equals(queryBuilder.value())) {
            assertTrue("Query should have been MatchNoDocsQuery but was " + query.getClass().getName(), query instanceof MatchNoDocsQuery);
        } else if (queryBuilder.fields().size() > 1) {
            assertTrue("Query should have been BooleanQuery but was " + query.getClass().getName(), query instanceof BooleanQuery);

            BooleanQuery boolQuery = (BooleanQuery) query;
            if (queryBuilder.lowercaseExpandedTerms()) {
                for (BooleanClause clause : boolQuery.clauses()) {
                    if (clause.getQuery() instanceof TermQuery) {
                        TermQuery inner = (TermQuery) clause.getQuery();
                        assertThat(inner.getTerm().bytes().toString(), is(inner.getTerm().bytes().toString().toLowerCase(Locale.ROOT)));
                    }
                }
            }

            assertThat(boolQuery.clauses().size(), equalTo(queryBuilder.fields().size()));
            Iterator<String> fields = queryBuilder.fields().keySet().iterator();
            for (BooleanClause booleanClause : boolQuery) {
                assertThat(booleanClause.getQuery(), instanceOf(TermQuery.class));
                TermQuery termQuery = (TermQuery) booleanClause.getQuery();
                assertThat(termQuery.getTerm(), equalTo(new Term(fields.next(), queryBuilder.value().toLowerCase(Locale.ROOT))));
            }

            if (queryBuilder.minimumShouldMatch() != null) {
                Collection<String> minMatchAlways = Arrays.asList("1", "-1", "75%", "-25%");
                Collection<String> minMatchLarger = Arrays.asList("2<75%", "2<-25%");

                if (minMatchAlways.contains(queryBuilder.minimumShouldMatch())) {
                    assertThat(boolQuery.getMinimumNumberShouldMatch(), greaterThan(0));
                } else if (minMatchLarger.contains(queryBuilder.minimumShouldMatch())) {
                    if (shouldClauses(boolQuery) > 2) {
                        assertThat(boolQuery.getMinimumNumberShouldMatch(), greaterThan(0));
                    }
                } else {
                    assertEquals(0, boolQuery.getMinimumNumberShouldMatch());
                }
            }
        } else if (queryBuilder.fields().size() <= 1) {
            assertTrue("Query should have been TermQuery but was " + query.getClass().getName(), query instanceof TermQuery);

            TermQuery termQuery = (TermQuery) query;
            String field;
            if (queryBuilder.fields().size() == 0) {
                field = MetaData.ALL;
            } else {
                field = queryBuilder.fields().keySet().iterator().next();
            }
            assertThat(termQuery.getTerm(), equalTo(new Term(field, queryBuilder.value().toLowerCase(Locale.ROOT))));

            if (queryBuilder.lowercaseExpandedTerms()) {
                assertThat(termQuery.getTerm().bytes().toString(), is(termQuery.getTerm().bytes().toString().toLowerCase(Locale.ROOT)));
            }
        } else {
            fail("Encountered lucene query type we do not have a validation implementation for in our " + SimpleQueryStringBuilderTests.class.getSimpleName());
        }
    }

    private int shouldClauses(BooleanQuery query) {
        int result = 0;
        for (BooleanClause c : query.clauses()) {
            if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                result++;
            }
        }
        return result;
    }
}
