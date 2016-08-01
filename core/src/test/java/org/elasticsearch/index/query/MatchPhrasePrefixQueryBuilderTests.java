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

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class MatchPhrasePrefixQueryBuilderTests extends AbstractQueryTestCase<MatchPhrasePrefixQueryBuilder> {
    @Override
    protected MatchPhrasePrefixQueryBuilder doCreateTestQueryBuilder() {
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

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(fieldName, value);

        if (randomBoolean()) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            matchQuery.slop(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.maxExpansions(randomIntBetween(1, 10000));
        }
        return matchQuery;
    }

    @Override
    protected void doAssertLuceneQuery(MatchPhrasePrefixQueryBuilder queryBuilder, Query query, QueryShardContext context)
            throws IOException {
        assertThat(query, notNullValue());
        assertThat(query,
                either(instanceOf(BooleanQuery.class)).or(instanceOf(MultiPhrasePrefixQuery.class))
                .or(instanceOf(TermQuery.class)).or(instanceOf(PointRangeQuery.class)).or(instanceOf(MatchNoDocsQuery.class)));
    }

    public void testIllegalValues() {
        try {
            new MatchPhrasePrefixQueryBuilder(null, "value");
            fail("value must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        try {
            new MatchPhrasePrefixQueryBuilder("fieldName", null);
            fail("value must not be non-null");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder("fieldName", "text");

        try {
            matchQuery.maxExpansions(-1);
            fail("must not be positive");
        } catch (IllegalArgumentException ex) {
            // expected
        }
    }

    public void testBadAnalyzer() throws IOException {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder("fieldName", "text");
        matchQuery.analyzer("bogusAnalyzer");
        try {
            matchQuery.toQuery(createShardContext());
            fail("Expected QueryShardException");
        } catch (QueryShardException e) {
            assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
        }
    }

    public void testPhrasePrefixMatchQuery() throws IOException {
        String json1 = "{\n" +
                "    \"match_phrase_prefix\" : {\n" +
                "        \"message\" : \"this is a test\"\n" +
                "    }\n" +
                "}";

        String expected = "{\n" +
                "  \"match_phrase_prefix\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"this is a test\",\n" +
                "      \"slop\" : 0,\n" +
                "      \"max_expansions\" : 50,\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MatchPhrasePrefixQueryBuilder qb = (MatchPhrasePrefixQueryBuilder) parseQuery(json1);
        checkGeneratedJson(expected, qb);

        String json3 = "{\n" +
                "    \"match_phrase_prefix\" : {\n" +
                "        \"message\" : {\n" +
                "            \"query\" : \"this is a test\",\n" +
                "            \"max_expansions\" : 10\n" +
                "        }\n" +
                "    }\n" +
                "}";
        expected = "{\n" +
                "  \"match_phrase_prefix\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"this is a test\",\n" +
                "      \"slop\" : 0,\n" +
                "      \"max_expansions\" : 10,\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        qb = (MatchPhrasePrefixQueryBuilder) parseQuery(json3);
        checkGeneratedJson(expected, qb);
    }
}
