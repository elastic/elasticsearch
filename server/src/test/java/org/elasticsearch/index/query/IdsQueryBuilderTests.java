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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class IdsQueryBuilderTests extends AbstractQueryTestCase<IdsQueryBuilder> {

    @Override
    protected IdsQueryBuilder doCreateTestQueryBuilder() {
        int numberOfIds = randomIntBetween(0, 10);
        String[] ids = new String[numberOfIds];
        for (int i = 0; i < numberOfIds; i++) {
            ids[i] = randomAlphaOfLengthBetween(1, 10);
        }
        IdsQueryBuilder query = new IdsQueryBuilder();
        query.addIds(ids);
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IdsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.ids().size() == 0) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(TermInSetQuery.class));
        }
    }

    public void testIllegalArguments() {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> idsQueryBuilder.addIds((String[]) null));
        assertEquals("[ids] ids cannot be null", e.getMessage());
    }

    // see #7686.
    public void testIdsQueryWithInvalidValues() throws Exception {
        String query = "{ \"ids\": { \"values\": [[1]] } }";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertThat(e.getMessage(), containsString("[ids] failed to parse field [values]"));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"ids\" : {\n" +
                "    \"values\" : [ \"1\", \"100\", \"4\" ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        IdsQueryBuilder parsed = (IdsQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertThat(parsed.ids(), contains("1","100","4"));

        // check that type that is not an array and also ids that are numbers are parsed
        json =
                "{\n" +
                "  \"ids\" : {\n" +
                "    \"values\" : [ 1, 100, 4 ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        parsed = (IdsQueryBuilder) parseQuery(json);
        assertThat(parsed.ids(), contains("1","100","4"));
    }

    @Override
    protected QueryBuilder parseQuery(XContentParser parser) throws IOException {
        QueryBuilder query = super.parseQuery(parser);
        assertThat(query, instanceOf(IdsQueryBuilder.class));
        return (IdsQueryBuilder) query;
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContextWithNoType();
        context.setAllowUnmappedFields(true);
        IdsQueryBuilder queryBuilder = createTestQueryBuilder();
        IllegalStateException e = expectThrows(IllegalStateException.class,
                () -> queryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
