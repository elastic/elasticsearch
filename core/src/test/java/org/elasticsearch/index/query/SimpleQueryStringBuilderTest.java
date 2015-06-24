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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.SimpleQueryParser.Settings;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.hamcrest.Matchers.*;

public class SimpleQueryStringBuilderTest extends BaseQueryTestCase<SimpleQueryStringBuilder> {

    private static final String[] MINIMUM_SHOULD_MATCH = new String[] { "1", "-1", "75%", "-25%", "2<75%", "2<-25%" };

    @Override
    protected SimpleQueryStringBuilder createTestQueryBuilder() {
        SimpleQueryStringBuilder result = new SimpleQueryStringBuilder(randomAsciiOfLengthBetween(1, 10));

        if (randomBoolean()) {
            result.queryName(randomAsciiOfLengthBetween(1, 10));
        }
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
            result.boost(2.0f / randomIntBetween(1, 20));
        }

        if (randomBoolean()) {
            Set<SimpleQueryStringFlag> flagSet = new HashSet<>();
            int size = randomIntBetween(0, SimpleQueryStringFlag.values().length);
            for (int i = 0; i < size; i++) {
                randomFrom(SimpleQueryStringFlag.values());
            }
            if (flagSet.size() > 0) {
                result.flags(flagSet.toArray(new SimpleQueryStringFlag[flagSet.size()]));
            }
        }

        int fieldCount = randomIntBetween(0, 10);
        Map<String, Float> fields = new TreeMap<>();
        for (int i = 0; i < fieldCount; i++) {
            if (randomBoolean()) {
                fields.put(randomAsciiOfLengthBetween(1, 10), 1.0f);
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

        assertEquals("Wrong default default boost.", 1.0f, qb.boost(), 0.001);
        assertEquals("Wrong default default boost field.", 1.0f, SimpleQueryStringBuilder.DEFAULT_BOOST, 0.001);

        assertEquals("Wrong default flags.", SimpleQueryStringFlag.ALL.value, qb.flags());
        assertEquals("Wrong default flags field.", SimpleQueryStringFlag.ALL.value(), SimpleQueryStringBuilder.DEFAULT_FLAGS);

        assertEquals("Wrong default default operator.", Operator.OR, qb.defaultOperator());
        assertEquals("Wrong default default operator field.", Operator.OR, SimpleQueryStringBuilder.DEFAULT_OPERATOR);

        assertEquals("Wrong default default locale.", Locale.ROOT, qb.locale());
        assertEquals("Wrong default default locale field.", Locale.ROOT, SimpleQueryStringBuilder.DEFAULT_LOCALE);

        assertEquals("Wrong default default analyze_wildcard.", false, qb.analyzeWildcard());
        assertEquals("Wrong default default analyze_wildcard field.", false, SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD);

        assertEquals("Wrong default default lowercase_expanded_terms.", true, qb.lowercaseExpandedTerms());
        assertEquals("Wrong default default lowercase_expanded_terms field.", true, SimpleQueryStringBuilder.DEFAULT_LOWERCASE_EXPANDED_TERMS);

        assertEquals("Wrong default default lenient.", false, qb.lenient());
        assertEquals("Wrong default default lenient field.", false, SimpleQueryStringBuilder.DEFAULT_LENIENT);

        assertEquals("Wrong default default locale.", Locale.ROOT, qb.locale());
        assertEquals("Wrong default default locale field.", Locale.ROOT, SimpleQueryStringBuilder.DEFAULT_LOCALE);
    }

    @Test
    public void testDefaultNullLocale() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.locale(null);
        assertEquals("Setting locale to null should result in returning to default value.", 
                SimpleQueryStringBuilder.DEFAULT_LOCALE, qb.locale());
    }

    @Test
    public void testDefaultNullComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags((SimpleQueryStringFlag[]) null);
        assertEquals("Setting flags to null should result in returning to default value.", 
                SimpleQueryStringBuilder.DEFAULT_FLAGS, qb.flags());
    }

    @Test
    public void testDefaultEmptyComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags(new SimpleQueryStringFlag[]{});
        assertEquals("Setting flags to empty should result in returning to default value.",
                SimpleQueryStringBuilder.DEFAULT_FLAGS, qb.flags());
    }

    @Test
    public void testDefaultNullComplainOp() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.defaultOperator(null);
        assertEquals("Setting operator to null should result in returning to default value.", 
                SimpleQueryStringBuilder.DEFAULT_OPERATOR, qb.defaultOperator());
    }

    // Check operator handling, and default field handling.
    @Test
    public void testDefaultOperatorHandling() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        BooleanQuery boolQuery = (BooleanQuery) qb.toQuery(createContext());
        assertThat(shouldClauses(boolQuery), is(4));

        qb.defaultOperator(Operator.AND);
        boolQuery = (BooleanQuery) qb.toQuery(createContext());
        assertThat(shouldClauses(boolQuery), is(0));

        qb.defaultOperator(Operator.OR);
        boolQuery = (BooleanQuery) qb.toQuery(createContext());
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

    @Test
    public void testHandlingDefaults() throws IOException {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.analyzer(null);
        qb.minimumShouldMatch(null);
        qb.queryName(null);
        assertEquals(qb.toQuery(createContext()), createExpectedQuery(qb, createContext()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeNullAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field(null, 1.0f);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeEmpty() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldCannotBeEmptyAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        qb.field("", 1.0f);
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

    @Override
    protected void assertLuceneQuery(SimpleQueryStringBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
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

    @Override
    protected Query createExpectedQuery(SimpleQueryStringBuilder queryBuilder, QueryParseContext context) throws IOException {
        Map<String, Float> fields = new TreeMap<>();
        // Use the default field (_all) if no fields specified
        if (queryBuilder.fields().isEmpty()) {
            String field = context.defaultField();
            fields.put(field, 1.0F);
        } else {
            fields.putAll(queryBuilder.fields());
        }

        // Use standard analyzer by default if none specified
        Analyzer luceneAnalyzer;
        if (queryBuilder.analyzer() == null) {
            luceneAnalyzer = context.mapperService().searchAnalyzer();
        } else {
            luceneAnalyzer = context.analysisService().analyzer(queryBuilder.analyzer());
        }
        SimpleQueryParser sqp = new SimpleQueryParser(luceneAnalyzer, fields, queryBuilder.flags(), new Settings(queryBuilder.locale(),
                queryBuilder.lowercaseExpandedTerms(), queryBuilder.lenient(), queryBuilder.analyzeWildcard()));

        if (queryBuilder.defaultOperator() != null) {
            switch (queryBuilder.defaultOperator()) {
            case OR:
                sqp.setDefaultOperator(Occur.SHOULD);
                break;
            case AND:
                sqp.setDefaultOperator(Occur.MUST);
                break;
            }
        }

        Query query = sqp.parse(queryBuilder.text());
        if (queryBuilder.queryName() != null) {
            context.addNamedQuery(queryBuilder.queryName(), query);
        }

        if (queryBuilder.minimumShouldMatch() != null && query instanceof BooleanQuery) {
            Queries.applyMinimumShouldMatch((BooleanQuery) query, queryBuilder.minimumShouldMatch());
        }
        query.setBoost(queryBuilder.boost());
        return query;
    }

}

