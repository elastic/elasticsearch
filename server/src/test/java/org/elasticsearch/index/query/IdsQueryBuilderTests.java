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


import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;

public class IdsQueryBuilderTests extends AbstractQueryTestCase<IdsQueryBuilder> {

    @Override
    protected IdsQueryBuilder doCreateTestQueryBuilder() {
        String[] types;
        if (getCurrentTypes() != null && getCurrentTypes().length > 0 && randomBoolean()) {
            int numberOfTypes = randomIntBetween(1, getCurrentTypes().length);
            types = new String[numberOfTypes];
            for (int i = 0; i < numberOfTypes; i++) {
                if (frequently()) {
                    types[i] = randomFrom(getCurrentTypes());
                } else {
                    types[i] = randomAlphaOfLengthBetween(1, 10);
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
            ids[i] = randomAlphaOfLengthBetween(1, 10);
        }
        IdsQueryBuilder query;
        if (types.length > 0 || randomBoolean()) {
            query = new IdsQueryBuilder().types(types);
            query.addIds(ids);
        } else {
            query = new IdsQueryBuilder();
            query.addIds(ids);
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        if (queryBuilder.ids().size() == 0 || context.getQueryShardContext().fieldMapper(UidFieldMapper.NAME) == null) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(TermInSetQuery.class));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IdsQueryBuilder().types((String[]) null));
        assertEquals("[ids] types cannot be null", e.getMessage());

        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        e = expectThrows(IllegalArgumentException.class, () -> idsQueryBuilder.addIds((String[]) null));
        assertEquals("[ids] ids cannot be null", e.getMessage());
    }

    // see #7686.
    public void testIdsQueryWithInvalidValues() throws Exception {
        String query = "{ \"ids\": { \"values\": [[1]] } }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[ids] failed to parse field [values]", e.getMessage());
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
        assertThat(parsed.ids(), contains("1","100","4"));
        assertEquals(json, "my_type", parsed.types()[0]);

        // check that type that is not an array and also ids that are numbers are parsed
        json =
                "{\n" +
                "  \"ids\" : {\n" +
                "    \"type\" : \"my_type\",\n" +
                "    \"values\" : [ 1, 100, 4 ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1","100","4"));
        assertEquals(json, "my_type", parsed.types()[0]);

        // check with empty type array
        json =
                "{\n" +
                "  \"ids\" : {\n" +
                "    \"type\" : [ ],\n" +
                "    \"values\" : [ \"1\", \"100\", \"4\" ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1","100","4"));
        assertEquals(json, 0, parsed.types().length);

        // check without type
        json =
                "{\n" +
                "  \"ids\" : {\n" +
                "    \"values\" : [ \"1\", \"100\", \"4\" ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1","100","4"));
        assertEquals(json, 0, parsed.types().length);
    }
}
