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

import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

public class IndicesQueryBuilderTests extends AbstractQueryTestCase<IndicesQueryBuilder> {

    @Override
    protected IndicesQueryBuilder doCreateTestQueryBuilder() {
        String[] indices;
        if (randomBoolean()) {
            indices = new String[]{getIndex().getName()};
        } else {
            indices = generateRandomStringArray(5, 10, false, false);
        }
        IndicesQueryBuilder query = new IndicesQueryBuilder(RandomQueryBuilder.createQuery(random()), indices);

        switch (randomInt(2)) {
            case 0:
                query.noMatchQuery(RandomQueryBuilder.createQuery(random()));
                break;
            case 1:
                query.noMatchQuery(randomFrom(QueryBuilders.matchAllQuery(), new MatchNoneQueryBuilder()));
                break;
            default:
                // do not set noMatchQuery
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(IndicesQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query expected;
        if (queryBuilder.indices().length == 1 && getIndex().getName().equals(queryBuilder.indices()[0])) {
            expected = queryBuilder.innerQuery().toQuery(context);
        } else {
            expected = queryBuilder.noMatchQuery().toQuery(context);
        }
        assertEquals(expected, query);
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new IndicesQueryBuilder(null, "index"));

        expectThrows(IllegalArgumentException.class, () -> new IndicesQueryBuilder(new MatchAllQueryBuilder(), (String[]) null));
        expectThrows(IllegalArgumentException.class, () -> new IndicesQueryBuilder(new MatchAllQueryBuilder(), new String[0]));

        IndicesQueryBuilder indicesQueryBuilder = new IndicesQueryBuilder(new MatchAllQueryBuilder(), "index");
        expectThrows(IllegalArgumentException.class, () -> indicesQueryBuilder.noMatchQuery((QueryBuilder) null));
        expectThrows(IllegalArgumentException.class, () -> indicesQueryBuilder.noMatchQuery((String) null));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"indices\" : {\n" +
                "    \"indices\" : [ \"index1\", \"index2\" ],\n" +
                "    \"query\" : {\n" +
                "      \"term\" : {\n" +
                "        \"tag\" : {\n" +
                "          \"value\" : \"wow\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"no_match_query\" : {\n" +
                "      \"term\" : {\n" +
                "        \"tag\" : {\n" +
                "          \"value\" : \"kow\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        IndicesQueryBuilder parsed = (IndicesQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 2, parsed.indices().length);
        assertEquals(json, "kow", ((TermQueryBuilder) parsed.noMatchQuery()).value());
        assertEquals(json, "wow", ((TermQueryBuilder) parsed.innerQuery()).value());
    }
}
