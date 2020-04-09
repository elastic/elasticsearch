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
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
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

public class MatchPhraseQueryBuilderTests extends AbstractQueryTestCase<MatchPhraseQueryBuilder> {
    @Override
    protected MatchPhraseQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME, BOOLEAN_FIELD_NAME, INT_FIELD_NAME,
                DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
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

        MatchPhraseQueryBuilder matchQuery = new MatchPhraseQueryBuilder(fieldName, value);

        if (randomBoolean() && isTextField(fieldName)) {
            matchQuery.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            matchQuery.slop(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            matchQuery.zeroTermsQuery(randomFrom(ZeroTermsQuery.ALL, ZeroTermsQuery.NONE));
        }

        return matchQuery;
    }

    @Override
    protected Map<String, MatchPhraseQueryBuilder> getAlternateVersions() {
        Map<String, MatchPhraseQueryBuilder> alternateVersions = new HashMap<>();
        MatchPhraseQueryBuilder matchPhraseQuery = new MatchPhraseQueryBuilder(randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        String contentString = "{\n" +
                "    \"match_phrase\" : {\n" +
                "        \"" + matchPhraseQuery.fieldName() + "\" : \"" + matchPhraseQuery.value() + "\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, matchPhraseQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MatchPhraseQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, notNullValue());

        if (query instanceof MatchAllDocsQuery) {
            assertThat(queryBuilder.zeroTermsQuery(), equalTo(ZeroTermsQuery.ALL));
            return;
        }

        assertThat(query, either(instanceOf(BooleanQuery.class)).or(instanceOf(PhraseQuery.class))
                .or(instanceOf(TermQuery.class)).or(instanceOf(PointRangeQuery.class))
                .or(instanceOf(IndexOrDocValuesQuery.class)).or(instanceOf(MatchNoDocsQuery.class)));
    }

    public void testIllegalValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchPhraseQueryBuilder(null, "value"));
        assertEquals("[match_phrase] requires fieldName", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new MatchPhraseQueryBuilder("fieldName", null));
        assertEquals("[match_phrase] requires query value", e.getMessage());
    }

    public void testBadAnalyzer() throws IOException {
        MatchPhraseQueryBuilder matchQuery = new MatchPhraseQueryBuilder("fieldName", "text");
        matchQuery.analyzer("bogusAnalyzer");
        QueryShardException e = expectThrows(QueryShardException.class, () -> matchQuery.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
    }

    public void testFromSimpleJson() throws IOException {
        String json1 = "{\n" +
                "    \"match_phrase\" : {\n" +
                "        \"message\" : \"this is a test\"\n" +
                "    }\n" +
                "}";

        String expected = "{\n" +
                "  \"match_phrase\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"this is a test\",\n" +
                "      \"slop\" : 0,\n" +
                "      \"zero_terms_query\" : \"NONE\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        MatchPhraseQueryBuilder qb = (MatchPhraseQueryBuilder) parseQuery(json1);
        checkGeneratedJson(expected, qb);
    }

    public void testFromJson() throws IOException {
        String json = "{\n" +
                "  \"match_phrase\" : {\n" +
                "    \"message\" : {\n" +
                "      \"query\" : \"this is a test\",\n" +
                "      \"slop\" : 2,\n" +
                "      \"zero_terms_query\" : \"ALL\",\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        MatchPhraseQueryBuilder parsed = (MatchPhraseQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "this is a test", parsed.value());
        assertEquals(json, 2, parsed.slop());
        assertEquals(json, ZeroTermsQuery.ALL, parsed.zeroTermsQuery());
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n" +
                "  \"match_phrase\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"query\" : \"this is a test\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"query\" : \"this is a test\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[match_phrase] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n" +
                "  \"match_phrase\" : {\n" +
                "    \"message1\" : \"this is a test\",\n" +
                "    \"message2\" : \"this is a test\"\n" +
                "  }\n" +
                "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[match_phrase] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }
}
