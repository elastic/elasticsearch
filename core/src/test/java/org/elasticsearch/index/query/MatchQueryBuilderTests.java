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

import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.LegacyNumericRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MatchQuery.Type;
import org.elasticsearch.index.search.MatchQuery.ZeroTermsQuery;
import org.hamcrest.Matcher;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MatchQueryBuilderTests extends AbstractQueryTestCase<MatchQueryBuilder> {
    @Override
    protected MatchQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME, BOOLEAN_FIELD_NAME, INT_FIELD_NAME,
                DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
        if (fieldName.equals(DATE_FIELD_NAME)) {
            assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        }
        Object value;
        if (fieldName.equals(STRING_FIELD_NAME)) {
            int terms = randomIntBetween(0, 3);
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < terms; i++) {
                builder.append(randomAsciiOfLengthBetween(1, 10)).append(" ");
            }
            value = builder.toString().trim();
        } else {
            value = getRandomValueForFieldName(fieldName);
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder(fieldName, value);
        matchQuery.operator(randomFrom(Operator.values()));

        if (randomBoolean()) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (fieldName.equals(BOOLEAN_FIELD_NAME) == false && randomBoolean()) {
            matchQuery.fuzziness(randomFuzziness(fieldName));
        }

        if (randomBoolean()) {
            matchQuery.prefixLength(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.maxExpansions(randomIntBetween(0, 1000));
        }

        if (randomBoolean()) {
            matchQuery.minimumShouldMatch(randomMinimumShouldMatch());
        }

        if (randomBoolean()) {
            matchQuery.fuzzyRewrite(getRandomRewriteMethod());
        }

        if (randomBoolean()) {
            matchQuery.fuzzyTranspositions(randomBoolean());
        }

        if (randomBoolean()) {
            matchQuery.lenient(randomBoolean());
        }

        if (randomBoolean()) {
            matchQuery.zeroTermsQuery(randomFrom(MatchQuery.ZeroTermsQuery.values()));
        }

        if (randomBoolean()) {
            matchQuery.cutoffFrequency((float) 10 / randomIntBetween(1, 100));
        }
        return matchQuery;
    }

    @Override
    protected void doAssertLuceneQuery(MatchQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQuery.ALL));
            return;
        }

        switch (queryBuilder.type()) {
        case BOOLEAN:
            assertThat(query, either(instanceOf(BooleanQuery.class)).or(instanceOf(ExtendedCommonTermsQuery.class))
                    .or(instanceOf(TermQuery.class)).or(instanceOf(FuzzyQuery.class))
                    .or(instanceOf(LegacyNumericRangeQuery.class)).or(instanceOf(PointRangeQuery.class)));
            break;
        case PHRASE:
            assertThat(query, either(instanceOf(BooleanQuery.class)).or(instanceOf(PhraseQuery.class))
                    .or(instanceOf(TermQuery.class)).or(instanceOf(FuzzyQuery.class))
                    .or(instanceOf(LegacyNumericRangeQuery.class)).or(instanceOf(PointRangeQuery.class)));
            break;
        case PHRASE_PREFIX:
            assertThat(query, either(instanceOf(BooleanQuery.class)).or(instanceOf(MultiPhrasePrefixQuery.class))
                    .or(instanceOf(TermQuery.class)).or(instanceOf(FuzzyQuery.class))
                    .or(instanceOf(LegacyNumericRangeQuery.class)).or(instanceOf(PointRangeQuery.class)));
            break;
        }

        MappedFieldType fieldType = context.fieldMapper(queryBuilder.fieldName());
        if (query instanceof TermQuery && fieldType != null) {
            String queryValue = queryBuilder.value().toString();
            if (queryBuilder.analyzer() == null || queryBuilder.analyzer().equals("simple")) {
                queryValue = queryValue.toLowerCase(Locale.ROOT);
            }
            Query expectedTermQuery = fieldType.termQuery(queryValue, context);
            assertEquals(expectedTermQuery, query);
        }

        if (query instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) query;
            if (queryBuilder.minimumShouldMatch() != null) {
                // calculate expected minimumShouldMatch value
                int optionalClauses = 0;
                for (BooleanClause c : bq.clauses()) {
                    if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                        optionalClauses++;
                    }
                }
                int msm = Queries.calculateMinShouldMatch(optionalClauses, queryBuilder.minimumShouldMatch());
                assertThat(bq.getMinimumNumberShouldMatch(), equalTo(msm));
            }
            if (queryBuilder.analyzer() == null && queryBuilder.value().toString().length() > 0) {
                assertEquals(bq.clauses().size(), queryBuilder.value().toString().split(" ").length);
            }
        }

        if (query instanceof ExtendedCommonTermsQuery) {
            assertTrue(queryBuilder.cutoffFrequency() != null);
            ExtendedCommonTermsQuery ectq = (ExtendedCommonTermsQuery) query;
            assertEquals(queryBuilder.cutoffFrequency(), ectq.getMaxTermFrequency(), Float.MIN_VALUE);
        }

        if (query instanceof FuzzyQuery) {
            assertTrue(queryBuilder.fuzziness() != null);
            FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
            // depending on analyzer being set or not we can have term lowercased along the way, so to simplify test we just
            // compare lowercased terms here
            String originalTermLc = queryBuilder.value().toString().toLowerCase(Locale.ROOT);
            String actualTermLc = fuzzyQuery.getTerm().text().toLowerCase(Locale.ROOT);
            Matcher<String> termLcMatcher = equalTo(originalTermLc);
            if ("false".equals(originalTermLc) || "true".equals(originalTermLc)) {
                // Booleans become t/f when querying a boolean field
                termLcMatcher = either(termLcMatcher).or(equalTo(originalTermLc.substring(0, 1)));
            }
            assertThat(actualTermLc, termLcMatcher);
            assertThat(queryBuilder.prefixLength(), equalTo(fuzzyQuery.getPrefixLength()));
            assertThat(queryBuilder.fuzzyTranspositions(), equalTo(fuzzyQuery.getTranspositions()));
        }

        if (query instanceof LegacyNumericRangeQuery) {
            // These are fuzzy numeric queries
            assertTrue(queryBuilder.fuzziness() != null);
            @SuppressWarnings("unchecked")
            LegacyNumericRangeQuery<Number> numericRangeQuery = (LegacyNumericRangeQuery<Number>) query;
            assertTrue(numericRangeQuery.includesMin());
            assertTrue(numericRangeQuery.includesMax());

            double value;
            double width;
            if (queryBuilder.fieldName().equals(DATE_FIELD_NAME) == false) {
                value = Double.parseDouble(queryBuilder.value().toString());
                if (queryBuilder.fuzziness().equals(Fuzziness.AUTO)) {
                    width = 1;
                } else {
                    width = queryBuilder.fuzziness().asDouble();
                }
            } else {
                value = ISODateTimeFormat.dateTimeParser().parseMillis(queryBuilder.value().toString());
                width = queryBuilder.fuzziness().asTimeValue().getMillis();
            }

            assertEquals(value - width, numericRangeQuery.getMin().doubleValue(), width * .1);
            assertEquals(value + width, numericRangeQuery.getMax().doubleValue(), width * .1);
        }

        if (query instanceof PointRangeQuery) {
            // TODO
        }
    }

    public void testIllegalValues() {
        try {
            new MatchQueryBuilder(null, "value");
            fail("value must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        try {
            new MatchQueryBuilder("fieldName", null);
            fail("value must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder("fieldName", "text");
        try {
            matchQuery.prefixLength(-1);
            fail("must not be positive");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        try {
            matchQuery.maxExpansions(-1);
            fail("must not be positive");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        try {
            matchQuery.operator(null);
            fail("must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        try {
            matchQuery.type(null);
            fail("must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        try {
            matchQuery.zeroTermsQuery(null);
            fail("must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }
    }

    public void testBadAnalyzer() throws IOException {
        MatchQueryBuilder matchQuery = new MatchQueryBuilder("fieldName", "text");
        matchQuery.analyzer("bogusAnalyzer");
        try {
            matchQuery.toQuery(createShardContext());
            fail("Expected QueryShardException");
        } catch (QueryShardException e) {
            assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
        }
    }

    public void testSimpleMatchQuery() throws IOException {
        String json = "{\n" +
                "  \"match\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"to be or not to be\",\n" +
                "      \"operator\" : \"AND\",\n" +
                "      \"prefix_length\" : 0,\n" +
                "      \"max_expansions\" : 50,\n" +
                "      \"fuzzy_transpositions\" : true,\n" +
                "      \"lenient\" : false,\n" +
                "      \"zero_terms_query\" : \"ALL\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MatchQueryBuilder qb = (MatchQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, qb);

        assertEquals(json, "to be or not to be", qb.value());
        assertEquals(json, Operator.AND, qb.operator());
    }

    public void testLegacyMatchPhrasePrefixQuery() throws IOException {
        MatchQueryBuilder expectedQB = new MatchQueryBuilder("message", "to be or not to be");
        expectedQB.type(Type.PHRASE_PREFIX);
        expectedQB.slop(2);
        expectedQB.maxExpansions(30);
        String json = "{\n" +
                "  \"match\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"to be or not to be\",\n" +
                "      \"type\" : \"phrase_prefix\",\n" +
                "      \"operator\" : \"OR\",\n" +
                "      \"slop\" : 2,\n" +
                "      \"prefix_length\" : 0,\n" +
                "      \"max_expansions\" : 30,\n" +
                "      \"fuzzy_transpositions\" : true,\n" +
                "      \"lenient\" : false,\n" +
                "      \"zero_terms_query\" : \"NONE\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MatchQueryBuilder qb = (MatchQueryBuilder) parseQuery(json, ParseFieldMatcher.EMPTY);
        checkGeneratedJson(json, qb);

        assertEquals(json, expectedQB, qb);

        assertSerialization(qb);

        // Now check with strict parsing an exception is thrown
        try {
            parseQuery(json, ParseFieldMatcher.STRICT);
            fail("Expected query to fail with strict parsing");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    containsString("Deprecated field [type] used, replaced by [match_phrase and match_phrase_prefix query]"));
        }
    }

    public void testLegacyMatchPhraseQuery() throws IOException {
        MatchQueryBuilder expectedQB = new MatchQueryBuilder("message", "to be or not to be");
        expectedQB.type(Type.PHRASE);
        expectedQB.slop(2);
        String json = "{\n" +
                "  \"match\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"to be or not to be\",\n" +
                "      \"type\" : \"phrase\",\n" +
                "      \"operator\" : \"OR\",\n" +
                "      \"slop\" : 2,\n" +
                "      \"prefix_length\" : 0,\n" +
                "      \"max_expansions\" : 50,\n" +
                "      \"fuzzy_transpositions\" : true,\n" +
                "      \"lenient\" : false,\n" +
                "      \"zero_terms_query\" : \"NONE\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MatchQueryBuilder qb = (MatchQueryBuilder) parseQuery(json, ParseFieldMatcher.EMPTY);
        checkGeneratedJson(json, qb);

        assertEquals(json, expectedQB, qb);

        assertSerialization(qb);

        // Now check with strict parsing an exception is thrown
        try {
            parseQuery(json, ParseFieldMatcher.STRICT);
            fail("Expected query to fail with strict parsing");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    containsString("Deprecated field [type] used, replaced by [match_phrase and match_phrase_prefix query]"));
        }
    }

    public void testLegacyFuzzyMatchQuery() throws IOException {
        MatchQueryBuilder expectedQB = new MatchQueryBuilder("message", "to be or not to be");
        String type = randomFrom("fuzzy_match", "match_fuzzy");
        if (randomBoolean()) {
            type = Strings.toCamelCase(type);
        }
        String json = "{\n" +
                "  \"" + type + "\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"to be or not to be\",\n" +
                "      \"operator\" : \"OR\",\n" +
                "      \"slop\" : 0,\n" +
                "      \"prefix_length\" : 0,\n" +
                "      \"max_expansions\" : 50,\n" +
                "      \"fuzzy_transpositions\" : true,\n" +
                "      \"lenient\" : false,\n" +
                "      \"zero_terms_query\" : \"NONE\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MatchQueryBuilder qb = (MatchQueryBuilder) parseQuery(json, ParseFieldMatcher.EMPTY);
        assertThat(qb, equalTo(expectedQB));

        // Now check with strict parsing an exception is thrown
        try {
            parseQuery(json, ParseFieldMatcher.STRICT);
            fail("Expected query to fail with strict parsing");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                    containsString("Deprecated field [" + type + "] used, expected [match] instead"));
        }
    }
}
