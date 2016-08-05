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
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.index.search.MatchQuery.Type;
import org.elasticsearch.index.search.MatchQuery.ZeroTermsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
            if (fieldName.equals(DATE_FIELD_NAME)) {
                // tokenized dates would trigger parse errors
                matchQuery.analyzer(randomFrom("keyword", "whitespace"));
            } else {
                matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
            }
        }

        if (fieldName.equals(STRING_FIELD_NAME) && randomBoolean()) {
            matchQuery.fuzziness(randomFuzziness(fieldName));
        }

        if (randomBoolean()) {
            matchQuery.prefixLength(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.maxExpansions(randomIntBetween(1, 1000));
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
    protected Map<String, MatchQueryBuilder> getAlternateVersions() {
        Map<String, MatchQueryBuilder> alternateVersions = new HashMap<>();
        MatchQueryBuilder matchQuery = new MatchQueryBuilder(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        String contentString = "{\n" +
                "    \"match\" : {\n" +
                "        \"" + matchQuery.fieldName() + "\" : \"" + matchQuery.value() + "\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, matchQuery);
        return alternateVersions;
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
                    .or(instanceOf(TermQuery.class)).or(instanceOf(FuzzyQuery.class)).or(instanceOf(MatchNoDocsQuery.class))
                    .or(instanceOf(PointRangeQuery.class)));
            break;
        case PHRASE:
            assertThat(query, either(instanceOf(BooleanQuery.class)).or(instanceOf(PhraseQuery.class))
                    .or(instanceOf(TermQuery.class)).or(instanceOf(FuzzyQuery.class))
                    .or(instanceOf(PointRangeQuery.class)));
            break;
        case PHRASE_PREFIX:
            assertThat(query, either(instanceOf(BooleanQuery.class)).or(instanceOf(MultiPhrasePrefixQuery.class))
                    .or(instanceOf(TermQuery.class)).or(instanceOf(FuzzyQuery.class))
                    .or(instanceOf(PointRangeQuery.class)));
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

        if (query instanceof PointRangeQuery) {
            // TODO
        }
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchQueryBuilder(null, "value"));
            assertEquals("[match] requires fieldName", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchQueryBuilder("fieldName", null));
            assertEquals("[match] requires query value", e.getMessage());
        }

        MatchQueryBuilder matchQuery = new MatchQueryBuilder("fieldName", "text");
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.prefixLength(-1));
            assertEquals("[match] requires prefix length to be non-negative.", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                    () -> matchQuery.maxExpansions(randomIntBetween(-10, 0)));
            assertEquals("[match] requires maxExpansions to be positive.", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.operator(null));
            assertEquals("[match] requires operator to be non-null", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.type(null));
            assertEquals("[match] requires type to be non-null", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> matchQuery.zeroTermsQuery(null));
            assertEquals("[match] requires zeroTermsQuery to be non-null", e.getMessage());
        }

        matchQuery.analyzer("bogusAnalyzer");
        {
            QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createShardContext()));
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(json, ParseFieldMatcher.STRICT));
        assertThat(e.getMessage(),
                containsString("Deprecated field [type] used, replaced by [match_phrase and match_phrase_prefix query]"));
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(json, ParseFieldMatcher.STRICT));
        assertThat(e.getMessage(),
                containsString("Deprecated field [type] used, replaced by [match_phrase and match_phrase_prefix query]"));
    }

    public void testLegacyFuzzyMatchQuery() throws IOException {
        MatchQueryBuilder expectedQB = new MatchQueryBuilder("message", "to be or not to be");
        String type = randomFrom("fuzzy_match", "match_fuzzy");
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(json, ParseFieldMatcher.STRICT));
        assertThat(e.getMessage(), containsString("Deprecated field [" + type + "] used, expected [match] instead"));
    }

    public void testFuzzinessOnNonStringField() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        MatchQueryBuilder query = new MatchQueryBuilder(INT_FIELD_NAME, 42);
        query.fuzziness(randomFuzziness(INT_FIELD_NAME));
        QueryShardContext context = createShardContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> query.toQuery(context));
        assertEquals("Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());
        query.analyzer("keyword"); // triggers a different code path
        e = expectThrows(IllegalArgumentException.class,
                () -> query.toQuery(context));
        assertEquals("Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());

        query.lenient(true);
        query.toQuery(context); // no exception
        query.analyzer(null);
        query.toQuery(context); // no exception
    }

    public void testExactOnUnsupportedField() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        MatchQueryBuilder query = new MatchQueryBuilder(GEO_POINT_FIELD_NAME, "2,3");
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.toQuery(context));
        assertEquals("Geo fields do not support exact searching, use dedicated geo queries instead: [mapped_geo_point]", e.getMessage());
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n" +
                "  \"match\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"query\" : \"this is a test\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"query\" : \"this is a test\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }
}
