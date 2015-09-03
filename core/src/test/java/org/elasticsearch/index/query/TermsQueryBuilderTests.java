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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.search.termslookup.TermsLookupFetchService;
import org.elasticsearch.indices.cache.query.terms.TermsLookup;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class TermsQueryBuilderTests extends BaseQueryTestCase<TermsQueryBuilder> {

    private MockTermsLookupFetchService termsLookupFetchService;

    @Before
    public void mockTermsLookupFetchService() {
        termsLookupFetchService = new MockTermsLookupFetchService();
        queryParserService().setTermsLookupFetchService(termsLookupFetchService);
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
            query = new TermsQueryBuilder(randomBoolean() ? randomAsciiOfLengthBetween(1,10) : STRING_FIELD_NAME);
            query.termsLookup(randomTermsLookup());
        }
        if (randomBoolean()) {
            query.minimumShouldMatch(randomInt(100) + "%");
        }
        if (randomBoolean()) {
            query.disableCoord(randomBoolean());
        }
        return query;
    }

    private TermsLookup randomTermsLookup() {
        return new TermsLookup(
                randomBoolean() ? randomAsciiOfLength(10) : null,
                randomAsciiOfLength(10),
                randomAsciiOfLength(10),
                randomAsciiOfLength(10)
        ).routing(randomBoolean() ? randomAsciiOfLength(10) : null);
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
            terms = termsLookupFetchService.getRandomTerms();
        } else {
            terms = queryBuilder.values();
        }

        // compare whether we have the expected list of terms returned
        Iterator<Object> iter = terms.iterator();
        for (BooleanClause booleanClause : booleanQuery) {
            assertThat(booleanClause.getQuery(), instanceOf(TermQuery.class));
            Term term = ((TermQuery) booleanClause.getQuery()).getTerm();
            Object next = iter.next();
            if (next == null) {
                continue;
            }
            assertThat(term, equalTo(new Term(queryBuilder.fieldName(), next.toString())));
        }
    }

    @Test
    public void testValidate() {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(null, "term");
        assertThat(termsQueryBuilder.validate().validationErrors().size(), is(1));

        termsQueryBuilder = new TermsQueryBuilder("field", "term").termsLookup(randomTermsLookup());
        assertThat(termsQueryBuilder.validate().validationErrors().size(), is(1));

        termsQueryBuilder = new TermsQueryBuilder(null, "term").termsLookup(randomTermsLookup());
        assertThat(termsQueryBuilder.validate().validationErrors().size(), is(2));

        termsQueryBuilder = new TermsQueryBuilder("field", "term");
        assertNull(termsQueryBuilder.validate());
    }

    @Test
    public void testValidateLookupQuery() {
        TermsQueryBuilder termsQuery = new TermsQueryBuilder("field").termsLookup(new TermsLookup());
        int totalExpectedErrors = 3;
        if (randomBoolean()) {
            termsQuery.lookupId("id");
            totalExpectedErrors--;
        }
        if (randomBoolean()) {
            termsQuery.lookupType("type");
            totalExpectedErrors--;
        }
        if (randomBoolean()) {
            termsQuery.lookupPath("path");
            totalExpectedErrors--;
        }
        assertValidate(termsQuery, totalExpectedErrors);
    }

    @Test
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

    @Test
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
        QueryBuilder termsQueryBuilder = parseQuery(query);
        assertThat(termsQueryBuilder.validate().validationErrors().size(), is(1));
    }

    private static class MockTermsLookupFetchService extends TermsLookupFetchService {

        private List<Object> randomTerms = new ArrayList<>();

        MockTermsLookupFetchService() {
            super(null, Settings.Builder.EMPTY_SETTINGS);
            String[] strings = generateRandomStringArray(10, 10, false, true);
            for (String string : strings) {
                randomTerms.add(string);
                if (rarely()) {
                    randomTerms.add(null);
                }
            }
        }

        @Override
        public List<Object> fetch(TermsLookup termsLookup) {
            return randomTerms;
        }

        List<Object> getRandomTerms() {
            return randomTerms;
        }
    }
}
