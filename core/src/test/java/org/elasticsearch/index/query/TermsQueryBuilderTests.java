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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.indices.cache.query.terms.TermsLookup;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TermsQueryBuilderTests extends AbstractQueryTestCase<TermsQueryBuilder> {
    private List<Object> randomTerms;
    private String termsPath;

    @Before
    public void randomTerms() {
        List<Object> randomTerms = new ArrayList<>();
        String[] strings = generateRandomStringArray(10, 10, false, true);
        for (String string : strings) {
            randomTerms.add(string);
            if (rarely()) {
                randomTerms.add(null);
            }
        }
        this.randomTerms = randomTerms;
        termsPath = randomAsciiOfLength(10).replace('.', '_');
    }

    @Override
    protected TermsQueryBuilder doCreateTestQueryBuilder() {
        TermsQueryBuilder query;
        // terms query or lookup query
        if (randomBoolean()) {
            // make between 0 and 5 different values of the same type
            String fieldName = getRandomFieldName();
            Object[] values = new Object[randomInt(5)];
            for (int i = 0; i < values.length; i++) {
                values[i] = getRandomValueForFieldName(fieldName);
            }
            query = new TermsQueryBuilder(fieldName, values);
        } else {
            // right now the mock service returns us a list of strings
            query = new TermsQueryBuilder(randomBoolean() ? randomAsciiOfLengthBetween(1,10) : STRING_FIELD_NAME, randomTermsLookup());
        }
        return query;
    }

    private TermsLookup randomTermsLookup() {
        return new TermsLookup(randomBoolean() ? randomAsciiOfLength(10) : null, randomAsciiOfLength(10), randomAsciiOfLength(10),
                termsPath).routing(randomBoolean() ? randomAsciiOfLength(10) : null);
    }

    @Override
    protected void doAssertLuceneQuery(TermsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;

        // we only do the check below for string fields (otherwise we'd have to decode the values)
        if (queryBuilder.fieldName().equals(INT_FIELD_NAME) || queryBuilder.fieldName().equals(DOUBLE_FIELD_NAME)
                || queryBuilder.fieldName().equals(BOOLEAN_FIELD_NAME) || queryBuilder.fieldName().equals(DATE_FIELD_NAME)) {
            return;
        }

        // expected returned terms depending on whether we have a terms query or a terms lookup query
        List<Object> terms;
        if (queryBuilder.termsLookup() != null) {
            terms = randomTerms;
        } else {
            terms = queryBuilder.values();
        }

        // compare whether we have the expected list of terms returned
        final List<Term> booleanTerms = new ArrayList<>();
        for (BooleanClause booleanClause : booleanQuery) {
            assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
            assertThat(booleanClause.getQuery(), instanceOf(TermQuery.class));
            Term term = ((TermQuery) booleanClause.getQuery()).getTerm();
            booleanTerms.add(term);
        }
        CollectionUtil.timSort(booleanTerms);
        List<Term> expectedTerms = new ArrayList<>();
        for (Object term : terms) {
            if (term != null) { // terms lookup filters this out
                expectedTerms.add(new Term(queryBuilder.fieldName(), term.toString()));
            }
        }
        CollectionUtil.timSort(expectedTerms);
        assertEquals(expectedTerms + " vs. " + booleanTerms, expectedTerms.size(), booleanTerms.size());
        assertEquals(expectedTerms + " vs. " + booleanTerms, expectedTerms, booleanTerms);
    }

    public void testEmtpyFieldName() {
        try {
            if (randomBoolean()) {
                new TermsQueryBuilder(null, "term");
            } else {
                new TermsQueryBuilder("", "term");
            }
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("field name cannot be null."));
        }
    }

    public void testEmtpyTermsLookup() {
        try {
            new TermsQueryBuilder("field", (TermsLookup) null);
            fail("Expected IllegalArgumentException");
        } catch(IllegalArgumentException e) {
            assertThat(e.getMessage(), is("No value or termsLookup specified for terms query"));
        }
    }

    public void testNullValues() {
        try {
            switch (randomInt(6)) {
                case 0:
                    new TermsQueryBuilder("field", (String[]) null);
                    break;
                case 1:
                    new TermsQueryBuilder("field", (int[]) null);
                    break;
                case 2:
                    new TermsQueryBuilder("field", (long[]) null);
                    break;
                case 3:
                    new TermsQueryBuilder("field", (float[]) null);
                    break;
                case 4:
                    new TermsQueryBuilder("field", (double[]) null);
                    break;
                case 5:
                    new TermsQueryBuilder("field", (Object[]) null);
                    break;
                default:
                    new TermsQueryBuilder("field", (Iterable<?>) null);
                    break;
            }
            fail("should have failed with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.containsString("No value specified for terms query"));
        }
    }

    public void testBothValuesAndLookupSet() throws IOException {
        String query = "{\n" +
                "  \"terms\": {\n" +
                "    \"field\": [\n" +
                "      \"blue\",\n" +
                "      \"pill\"\n" +
                "    ],\n" +
                "    \"field_lookup\": {\n" +
                "      \"index\": \"pills\",\n" +
                "      \"type\": \"red\",\n" +
                "      \"id\": \"3\",\n" +
                "      \"path\": \"white rabbit\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        try {
            parseQuery(query);
            fail("Expected ParsingException");
        } catch(ParsingException e) {
            assertThat(e.getMessage(), containsString("[" + TermsQueryBuilder.NAME + "] query does not support more than one field."));
        }
    }

    @Override
    public GetResponse executeGet(GetRequest getRequest) {
        String json;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            builder.array(termsPath, randomTerms.toArray(new Object[randomTerms.size()]));
            builder.endObject();
            json = builder.string();
        } catch (IOException ex) {
            throw new ElasticsearchException("boom", ex);
        }
        return new GetResponse(new GetResult(getRequest.index(), getRequest.type(), getRequest.id(), 0, true, new BytesArray(json), null));
    }

    public void testNumeric() throws IOException {
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new int[]{1, 3, 4});
            TermsQueryBuilder copy = assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1, 3, 4), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new double[]{1, 3, 4});
            TermsQueryBuilder copy = assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1d, 3d, 4d), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new float[]{1, 3, 4});
            TermsQueryBuilder copy = assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1f, 3f, 4f), values);
        }
        {
            TermsQueryBuilder builder = new TermsQueryBuilder("foo", new long[]{1, 3, 4});
            TermsQueryBuilder copy = assertSerialization(builder);
            List<Object> values = copy.values();
            assertEquals(Arrays.asList(1l, 3l, 4l), values);
        }
    }

    public void testTermsQueryWithMultipleFields() throws IOException {
        String query = XContentFactory.jsonBuilder().startObject()
                .startObject("terms").array("foo", 123).array("bar", 456).endObject()
                .endObject().string();
        try {
            parseQuery(query);
            fail("parsing should have failed");
        } catch (ParsingException ex) {
            assertThat(ex.getMessage(), equalTo("[" + TermsQueryBuilder.NAME + "] query does not support multiple fields"));
        }
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"terms\" : {\n" +
                "    \"user\" : [ \"kimchy\", \"elasticsearch\" ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        TermsQueryBuilder parsed = (TermsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 2, parsed.values().size());
    }
}

