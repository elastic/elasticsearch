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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.index.search.MatchQuery.ZeroTermsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class MatchPhrasePrefixQueryBuilderTests extends AbstractQueryTestCase<MatchPhrasePrefixQueryBuilder> {
    @Override
    protected MatchPhrasePrefixQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
        Object value;
        if (isTextField(fieldName)) {
            int terms = randomIntBetween(0, 3);
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < terms; i++) {
                builder.append(randomAlphaOfLengthBetween(1, 10)).append(" ");
            }
            value = builder.toString().trim();
        } else {
            value = getRandomValueForFieldName(fieldName);
        }

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(fieldName, value);

        if (randomBoolean() && isTextField(fieldName)) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            matchQuery.slop(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.maxExpansions(randomIntBetween(1, 10000));
        }
        if (randomBoolean()) {
            matchQuery.zeroTermsQuery(randomFrom(ZeroTermsQuery.ALL, ZeroTermsQuery.NONE));
        }
        return matchQuery;
    }

    @Override
    protected Map<String, MatchPhrasePrefixQueryBuilder> getAlternateVersions() {
        Map<String, MatchPhrasePrefixQueryBuilder> alternateVersions = new HashMap<>();
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQuery = new MatchPhrasePrefixQueryBuilder(randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        String contentString = "{\n" +
                "    \"match_phrase_prefix\" : {\n" +
                "        \"" + matchPhrasePrefixQuery.fieldName() + "\" : \"" + matchPhrasePrefixQuery.value() + "\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, matchPhrasePrefixQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchPhrasePrefixQueryBuilder queryBuilder, Query query, QueryShardContext context)
            throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQuery.ALL));
            return;
        }

        assertThat(query, either(instanceOf(MultiPhrasePrefixQuery.class))
            .or(instanceOf(SynonymQuery.class))
            .or(instanceOf(MatchNoDocsQuery.class)));
    }

    public void testIllegalValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchPhrasePrefixQueryBuilder(null, "value"));
        assertEquals("[match_phrase_prefix] requires fieldName", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new MatchPhrasePrefixQueryBuilder("fieldName", null));
        assertEquals("[match_phrase_prefix] requires query value", e.getMessage());

        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder("fieldName", "text");
        e = expectThrows(IllegalArgumentException.class, () -> matchQuery.maxExpansions(-1));
    }

    public void testBadAnalyzer() throws IOException {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder("fieldName", "text");
        matchQuery.analyzer("bogusAnalyzer");

        QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
    }

    public void testPhraseOnFieldWithNoTerms() {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(DATE_FIELD_NAME, "three term phrase");
        matchQuery.analyzer("whitespace");
        expectThrows(IllegalStateException.class, () -> matchQuery.doToQuery(createShardContext()));
    }

    public void testPhrasePrefixZeroTermsQuery() throws IOException {
        MatchPhrasePrefixQueryBuilder matchQuery = new MatchPhrasePrefixQueryBuilder(TEXT_FIELD_NAME, "");
        matchQuery.zeroTermsQuery(ZeroTermsQuery.NONE);
        assertEquals(new MatchNoDocsQuery(), matchQuery.doToQuery(createShardContext()));

        matchQuery = new MatchPhrasePrefixQueryBuilder(TEXT_FIELD_NAME, "");
        matchQuery.zeroTermsQuery(ZeroTermsQuery.ALL);
        assertEquals(new MatchAllDocsQuery(), matchQuery.doToQuery(createShardContext()));
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
                "      \"zero_terms_query\" : \"NONE\",\n" +
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
                "      \"zero_terms_query\" : \"NONE\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        qb = (MatchPhrasePrefixQueryBuilder) parseQuery(json3);
        checkGeneratedJson(expected, qb);
    }


    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n" +
                "  \"match_phrase_prefix\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"query\" : \"this is a test\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"query\" : \"this is a test\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match_phrase_prefix] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n" +
                "  \"match_phrase_prefix\" : {\n" +
                "    \"message1\" : \"this is a test\",\n" +
                "    \"message2\" : \"this is a test\"\n" +
                "  }\n" +
                "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[match_phrase_prefix] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }
}
