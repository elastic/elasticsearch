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
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;

public class IdsQueryBuilderTests extends AbstractQueryTestCase<IdsQueryBuilder> {
    /**
     * Check that parser throws exception on missing values field.
     */
    public void testIdsNotProvided() throws IOException {
        String noIdsFieldQuery = "{\"ids\" : { \"type\" : \"my_type\"  }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(noIdsFieldQuery));
        assertThat(e.getMessage(), containsString("no ids values provided"));
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

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IdsQueryBuilder((String[]) null));
        assertEquals("[ids] types cannot be null", e.getMessage());

        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        e = expectThrows(IllegalArgumentException.class, () -> idsQueryBuilder.addIds((String[])null));
        assertEquals("[ids] ids cannot be null", e.getMessage());
    }

    // see #7686.
    public void testIdsQueryWithInvalidValues() throws Exception {
        String query = "{ \"ids\": { \"values\": [[1]] } }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("Illegal value for id, expecting a string or number, got: START_ARRAY", e.getMessage());
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

    public void testFromJsonDeprecatedSyntax() throws IOException {
        IdsQueryBuilder tempQuery = createTestQueryBuilder();
        assumeTrue("test requires at least one type", tempQuery.types() != null && tempQuery.types().length > 0);

        String type = tempQuery.types()[0];
        IdsQueryBuilder testQuery = new IdsQueryBuilder(type);

        //single value type can also be called _type
        final String contentString = "{\n" +
                "    \"ids\" : {\n" +
                "        \"_type\" : \"" + type + "\",\n" +
                "        \"values\" : []\n" +
                "    }\n" +
                "}";

        IdsQueryBuilder parsed = (IdsQueryBuilder) parseQuery(contentString, ParseFieldMatcher.EMPTY);
        assertEquals(testQuery, parsed);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(contentString));
        assertEquals("Deprecated field [_type] used, expected [type] instead", e.getMessage());

        //array of types can also be called type rather than types
        final String contentString2 = "{\n" +
                "    \"ids\" : {\n" +
                "        \"types\" : [\"" + type + "\"],\n" +
                "        \"values\" : []\n" +
                "    }\n" +
                "}";
        parsed = (IdsQueryBuilder) parseQuery(contentString, ParseFieldMatcher.EMPTY);
        assertEquals(testQuery, parsed);

        e = expectThrows(IllegalArgumentException.class, () -> parseQuery(contentString2));
        assertEquals("Deprecated field [types] used, expected [type] instead", e.getMessage());
    }
}
