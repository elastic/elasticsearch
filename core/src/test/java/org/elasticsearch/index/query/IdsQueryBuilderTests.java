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


import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class IdsQueryBuilderTests extends AbstractQueryTestCase<IdsQueryBuilder> {
    /**
     * Check that parser throws exception on missing values field.
     */
    public void testIdsNotProvided() throws IOException {
        String noIdsFieldQuery = "{\"ids\" : { \"type\" : \"my_type\"  }";
        try {
            parseQuery(noIdsFieldQuery);
            fail("Expected ParsingException");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("no ids values provided"));
        }
    }

    @Override
    protected IdsQueryBuilder doCreateTestQueryBuilder() {
        String[] types;
        if (getCurrentTypes().length > 0 && randomBoolean()) {
            int numberOfTypes = randomIntBetween(1, getCurrentTypes().length);
            types = new String[numberOfTypes];
            for (int i = 0; i < numberOfTypes; i++) {
                if (frequently()) {
                    types[i] = randomFrom(getCurrentTypes());
                } else {
                    types[i] = randomAsciiOfLengthBetween(1, 10);
                }
            }
        } else {
            if (randomBoolean()) {
                types = new String[]{MetaData.ALL};
            } else {
                types = new String[0];
            }
        }
        int numberOfIds = randomIntBetween(0, 10);
        String[] ids = new String[numberOfIds];
        for (int i = 0; i < numberOfIds; i++) {
            ids[i] = randomAsciiOfLengthBetween(1, 10);
        }
        IdsQueryBuilder query;
        if (types.length > 0 || randomBoolean()) {
            query = new IdsQueryBuilder(types);
            query.addIds(ids);
        } else {
            query = new IdsQueryBuilder();
            query.addIds(ids);
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.ids().size() == 0) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(TermsQuery.class));
        }
    }

    @Override
    protected Map<String, IdsQueryBuilder> getAlternateVersions() {
        Map<String, IdsQueryBuilder> alternateVersions = new HashMap<>();

        IdsQueryBuilder tempQuery = createTestQueryBuilder();
        if (tempQuery.types() != null && tempQuery.types().length > 0) {
            String type = tempQuery.types()[0];
            IdsQueryBuilder testQuery = new IdsQueryBuilder(type);

            //single value type can also be called _type
            String contentString1 = "{\n" +
                        "    \"ids\" : {\n" +
                        "        \"_type\" : \"" + type + "\",\n" +
                        "        \"values\" : []\n" +
                        "    }\n" +
                        "}";
            alternateVersions.put(contentString1, testQuery);

            //array of types can also be called type rather than types
            String contentString2 = "{\n" +
                        "    \"ids\" : {\n" +
                        "        \"type\" : [\"" + type + "\"],\n" +
                        "        \"values\" : []\n" +
                        "    }\n" +
                        "}";
            alternateVersions.put(contentString2, testQuery);
        }

        return alternateVersions;
    }

    public void testIllegalArguments() {
        try {
            new IdsQueryBuilder((String[])null);
            fail("must be not null");
        } catch(IllegalArgumentException e) {
            //all good
        }

        try {
            new IdsQueryBuilder().addIds((String[])null);
            fail("must be not null");
        } catch(IllegalArgumentException e) {
            //all good
        }
    }

    // see #7686.
    public void testIdsQueryWithInvalidValues() throws Exception {
        String query = "{ \"ids\": { \"values\": [[1]] } }";
        try {
            parseQuery(query);
            fail("Expected ParsingException");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), is("Illegal value for id, expecting a string or number, got: START_ARRAY"));
        }
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"ids\" : {\n" +
                "    \"type\" : [ \"my_type\" ],\n" +
                "    \"values\" : [ \"1\", \"100\", \"4\" ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        IdsQueryBuilder parsed = (IdsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 3, parsed.ids().size());
        assertEquals(json, "my_type", parsed.types()[0]);
    }
}
