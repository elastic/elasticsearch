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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FuzzyQueryBuilderTests extends AbstractQueryTestCase<FuzzyQueryBuilder> {

    @Override
    protected FuzzyQueryBuilder doCreateTestQueryBuilder() {
        FuzzyQueryBuilder query = new FuzzyQueryBuilder(STRING_FIELD_NAME, getRandomValueForFieldName(STRING_FIELD_NAME));
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
        FuzzyQueryBuilder fuzzyQuery = new FuzzyQueryBuilder(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
        String contentString = "{\n" +
                "    \"fuzzy\" : {\n" +
                "        \"" + fuzzyQuery.fieldName() + "\" : \"" + fuzzyQuery.value() + "\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(contentString, fuzzyQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(FuzzyQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(FuzzyQuery.class));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder(null, "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("", "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("field", null));
        assertEquals("query value cannot be null", e.getMessage());
    }

    public void testUnsupportedFuzzinessForStringType() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        FuzzyQueryBuilder fuzzyQueryBuilder = new FuzzyQueryBuilder(STRING_FIELD_NAME, "text");
        fuzzyQueryBuilder.fuzziness(Fuzziness.build(randomFrom("a string which is not auto", "3h", "200s")));
        NumberFormatException e = expectThrows(NumberFormatException.class, () -> fuzzyQueryBuilder.toQuery(context));
        assertThat(e.getMessage(), containsString("For input string"));
    }

    public void testToQueryWithStringField() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
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

    public void testToQueryWithNumericField() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        String query = "{\n" +
                "    \"fuzzy\":{\n" +
                "        \"" + INT_FIELD_NAME + "\":{\n" +
                "            \"value\":12,\n" +
                "            \"fuzziness\":5\n" +
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
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n" +
                "  \"fuzzy\" : {\n" +
                "    \"message1\" : {\n" +
                "      \"value\" : \"this is a test\"\n" +
                "    },\n" +
                "    \"message2\" : {\n" +
                "      \"value\" : \"this is a test\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }
}
