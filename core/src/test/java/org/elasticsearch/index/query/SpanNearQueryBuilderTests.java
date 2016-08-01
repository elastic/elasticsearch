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
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanNearQueryBuilderTests extends AbstractQueryTestCase<SpanNearQueryBuilder> {
    @Override
    protected SpanNearQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(randomIntBetween(1, 6));
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(spanTermQueries[0], randomIntBetween(-10, 10));
        for (int i = 1; i < spanTermQueries.length; i++) {
            queryBuilder.addClause(spanTermQueries[i]);
        }
        queryBuilder.inOrder(randomBoolean());
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(SpanNearQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) query;
        assertThat(spanNearQuery.getSlop(), equalTo(queryBuilder.slop()));
        assertThat(spanNearQuery.isInOrder(), equalTo(queryBuilder.inOrder()));
        assertThat(spanNearQuery.getClauses().length, equalTo(queryBuilder.clauses().size()));
        Iterator<SpanQueryBuilder> spanQueryBuilderIterator = queryBuilder.clauses().iterator();
        for (SpanQuery spanQuery : spanNearQuery.getClauses()) {
            assertThat(spanQuery, equalTo(spanQueryBuilderIterator.next().toQuery(context)));
        }
    }

    public void testIllegalArguments() {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SpanNearQueryBuilder(null, 1));
            assertEquals("[span_near] must include at least one clause", e.getMessage());

            SpanNearQueryBuilder spanNearQueryBuilder = new SpanNearQueryBuilder(new SpanTermQueryBuilder("field", "value"), 1);
            e = expectThrows(IllegalArgumentException.class, () -> spanNearQueryBuilder.addClause(null));
            assertEquals("[span_near]  clauses cannot be null", e.getMessage());
    }

    public void testClausesUnmodifiable() {
        SpanNearQueryBuilder spanNearQueryBuilder = new SpanNearQueryBuilder(new SpanTermQueryBuilder("field", "value"), 1);
        expectThrows(UnsupportedOperationException.class,
                () -> spanNearQueryBuilder.clauses().add(new SpanTermQueryBuilder("field", "value2")));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"span_near\" : {\n" +
                "    \"clauses\" : [ {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value1\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value2\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value3\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"slop\" : 12,\n" +
                "    \"in_order\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        SpanNearQueryBuilder parsed = (SpanNearQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 3, parsed.clauses().size());
        assertEquals(json, 12, parsed.slop());
        assertEquals(json, false, parsed.inOrder());
    }

    public void testCollectPayloadsDeprecated() throws Exception {
        assertEquals("We can remove support for ignoring collect_payloads in 6.0.0", 5, Version.CURRENT.major);
        String json =
                "{\n" +
                "  \"span_near\" : {\n" +
                "    \"clauses\" : [ {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value1\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value2\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value3\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"slop\" : 12,\n" +
                "    \"in_order\" : false,\n" +
                "    \"collect_payloads\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        parseQuery(json, ParseFieldMatcher.EMPTY); // Just don't throw an error and we're fine
    }
}
