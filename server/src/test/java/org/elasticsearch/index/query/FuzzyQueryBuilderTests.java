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
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FuzzyQueryBuilderTests extends AbstractQueryTestCase<FuzzyQueryBuilder> {

    @Override
    protected FuzzyQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME, STRING_ALIAS_FIELD_NAME);
        FuzzyQueryBuilder query = new FuzzyQueryBuilder(fieldName, getRandomValueForFieldName(fieldName));
        if (randomBoolean()) {
            query.fuzziness(randomFuzziness(query.fieldName()));
        }
        if (randomBoolean()) {
            query.prefixLength(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            query.maxExpansions(randomIntBetween(1, 10));
        }
        if (randomBoolean()) {
            query.transpositions(randomBoolean());
        }
        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected Map<String, FuzzyQueryBuilder> getAlternateVersions() {
        Map<String, FuzzyQueryBuilder> alternateVersions = new HashMap<>();
        FuzzyQueryBuilder fuzzyQuery = new FuzzyQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
        String contentString = "{\n" +
                "    \"fuzzy\" : {\n" +
                "        \"" + fuzzyQuery.fieldName() + "\" : \"" + fuzzyQuery.value() + "\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, fuzzyQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(FuzzyQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(FuzzyQuery.class));

        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
        String actualFieldName = fuzzyQuery.getTerm().field();
        assertThat(actualFieldName, equalTo(expectedFieldName));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder(null, "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("", "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("field", null));
        assertEquals("query value cannot be null", e.getMessage());
    }

    public void testToQueryWithStringField() throws IOException {
        String query = "{\n" +
                "    \"fuzzy\":{\n" +
                "        \"" + STRING_FIELD_NAME + "\":{\n" +
                "            \"value\":\"sh\",\n" +
                "            \"fuzziness\": \"AUTO\",\n" +
                "            \"prefix_length\":1,\n" +
                "            \"boost\":2.0\n" +
                "        }\n" +
                "    }\n" +
                "}";
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) parsedQuery;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) boostQuery.getQuery();
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(STRING_FIELD_NAME, "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(Fuzziness.AUTO.asDistance("sh")));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
    }

    public void testToQueryWithStringFieldDefinedFuzziness() throws IOException {
        String query = "{\n" +
            "    \"fuzzy\":{\n" +
            "        \"" + STRING_FIELD_NAME + "\":{\n" +
            "            \"value\":\"sh\",\n" +
            "            \"fuzziness\": \"AUTO:2,5\",\n" +
            "            \"prefix_length\":1,\n" +
            "            \"boost\":2.0\n" +
            "        }\n" +
            "    }\n" +
            "}";
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) parsedQuery;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) boostQuery.getQuery();
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(STRING_FIELD_NAME, "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(1));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
    }

    public void testToQueryWithStringFieldDefinedWrongFuzziness() throws IOException {
        String queryMissingFuzzinessUpLimit = "{\n" +
            "    \"fuzzy\":{\n" +
            "        \"" + STRING_FIELD_NAME + "\":{\n" +
            "            \"value\":\"sh\",\n" +
            "            \"fuzziness\": \"AUTO:2\",\n" +
            "            \"prefix_length\":1,\n" +
            "            \"boost\":2.0\n" +
            "        }\n" +
            "    }\n" +
            "}";
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessUpLimit).toQuery(createShardContext()));
        String msg = "failed to find low and high distance values";
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryHavingNegativeFuzzinessLowLimit = "{\n" +
            "    \"fuzzy\":{\n" +
            "        \"" + STRING_FIELD_NAME + "\":{\n" +
            "            \"value\":\"sh\",\n" +
            "            \"fuzziness\": \"AUTO:-1,6\",\n" +
            "            \"prefix_length\":1,\n" +
            "            \"boost\":2.0\n" +
            "        }\n" +
            "    }\n" +
            "}";
        String msg2 = "fuzziness wrongly configured";
        ElasticsearchParseException e2 = expectThrows(ElasticsearchParseException.class,
            () -> parseQuery(queryHavingNegativeFuzzinessLowLimit).toQuery(createShardContext()));
        assertTrue(e2.getMessage() + " didn't contain: " + msg2 + " but: " + e.getMessage(), e.getMessage().contains
            (msg));

        String queryMissingFuzzinessUpLimit2 = "{\n" +
            "    \"fuzzy\":{\n" +
            "        \"" + STRING_FIELD_NAME + "\":{\n" +
            "            \"value\":\"sh\",\n" +
            "            \"fuzziness\": \"AUTO:1,\",\n" +
            "            \"prefix_length\":1,\n" +
            "            \"boost\":2.0\n" +
            "        }\n" +
            "    }\n" +
            "}";
        e = expectThrows(ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessUpLimit2).toQuery(createShardContext()));
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryMissingFuzzinessLowLimit = "{\n" +
            "    \"fuzzy\":{\n" +
            "        \"" + STRING_FIELD_NAME + "\":{\n" +
            "            \"value\":\"sh\",\n" +
            "            \"fuzziness\": \"AUTO:,5\",\n" +
            "            \"prefix_length\":1,\n" +
            "            \"boost\":2.0\n" +
            "        }\n" +
            "    }\n" +
            "}";
        e = expectThrows(ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessLowLimit).toQuery(createShardContext()));
        msg = "failed to parse [AUTO:,5] as a \"auto:int,int\"";
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));
    }

    public void testToQueryWithNumericField() throws IOException {
        String query = "{\n" +
                "    \"fuzzy\":{\n" +
                "        \"" + INT_FIELD_NAME + "\":{\n" +
                "            \"value\":12,\n" +
                "            \"fuzziness\":2\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> parseQuery(query).toQuery(createShardContext()));
        assertEquals("Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"fuzzy\" : {\n" +
                "    \"user\" : {\n" +
                "      \"value\" : \"ki\",\n" +
                "      \"fuzziness\" : \"2\",\n" +
                "      \"prefix_length\" : 0,\n" +
                "      \"max_expansions\" : 100,\n" +
                "      \"transpositions\" : false,\n" +
                "      \"boost\" : 42.0\n" +
                "    }\n" +
                "  }\n" +
                "}";
        FuzzyQueryBuilder parsed = (FuzzyQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.00001);
        assertEquals(json, 2, parsed.fuzziness().asFloat(), 0f);
        assertEquals(json, false, parsed.transpositions());
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json1 = "{\n" +
                "  \"fuzzy\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"value\" : \"this is a test\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        parseQuery(json1); // should be all good

        String json2 = "{\n" +
                "  \"fuzzy\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"value\" : \"this is a test\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"value\" : \"this is a test\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json2));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n" +
                "  \"fuzzy\" : {\n" +
                "    \"message1\" : \"this is a test\",\n" +
                "    \"message2\" : \"value\" : \"this is a test\"\n" +
                "  }\n" +
                "}";

        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseFailsWithValueArray() {
        String query = "{\n" +
                "  \"fuzzy\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"value\" : [ \"one\", \"two\", \"three\"]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[fuzzy] unexpected token [START_ARRAY] after [value]", e.getMessage());
    }

    public void testToQueryWithTranspositions() throws Exception {
        Query query = new FuzzyQueryBuilder(STRING_FIELD_NAME, "text").toQuery(createShardContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(FuzzyQuery.defaultTranspositions, ((FuzzyQuery)query).getTranspositions());

        query = new FuzzyQueryBuilder(STRING_FIELD_NAME, "text").transpositions(true).toQuery(createShardContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(true, ((FuzzyQuery)query).getTranspositions());

        query = new FuzzyQueryBuilder(STRING_FIELD_NAME, "text").transpositions(false).toQuery(createShardContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(false, ((FuzzyQuery)query).getTranspositions());
    }
}
