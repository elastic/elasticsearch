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
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanContainingQueryBuilderTests extends AbstractQueryTestCase<SpanContainingQueryBuilder> {
    @Override
    protected SpanContainingQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(2);
        return new SpanContainingQueryBuilder(spanTermQueries[0], spanTermQueries[1]);
    }

    @Override
    protected void doAssertLuceneQuery(SpanContainingQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        assertThat(query, instanceOf(SpanContainingQuery.class));
    }

    public void testIllegalArguments() {
        SpanTermQueryBuilder spanTermQuery = new SpanTermQueryBuilder("field", "value");
        expectThrows(IllegalArgumentException.class, () -> new SpanContainingQueryBuilder(null, spanTermQuery));
        expectThrows(IllegalArgumentException.class, () -> new SpanContainingQueryBuilder(spanTermQuery, null));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"span_containing\" : {\n" +
                "    \"big\" : {\n" +
                "      \"span_near\" : {\n" +
                "        \"clauses\" : [ {\n" +
                "          \"span_term\" : {\n" +
                "            \"field1\" : {\n" +
                "              \"value\" : \"bar\",\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"span_term\" : {\n" +
                "            \"field1\" : {\n" +
                "              \"value\" : \"baz\",\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        } ],\n" +
                "        \"slop\" : 5,\n" +
                "        \"in_order\" : true,\n" +
                "        \"boost\" : 1.0\n" +
                "      }\n" +
                "    },\n" +
                "    \"little\" : {\n" +
                "      \"span_term\" : {\n" +
                "        \"field1\" : {\n" +
                "          \"value\" : \"foo\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"boost\" : 2.0\n" +
                "  }\n" +
                "}";

        SpanContainingQueryBuilder parsed = (SpanContainingQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 2, ((SpanNearQueryBuilder) parsed.bigQuery()).clauses().size());
        assertEquals(json, "foo", ((SpanTermQueryBuilder) parsed.littleQuery()).value());
        assertEquals(json, 2.0, parsed.boost(), 0.0);
    }

    public void testFromJsoWithNonDefaultBoostInBigQuery() {
        String json =
                "{\n" +
                "  \"span_containing\" : {\n" +
                "    \"big\" : {\n" +
                "      \"span_near\" : {\n" +
                "        \"clauses\" : [ {\n" +
                "          \"span_term\" : {\n" +
                "            \"field1\" : {\n" +
                "              \"value\" : \"bar\",\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"span_term\" : {\n" +
                "            \"field1\" : {\n" +
                "              \"value\" : \"baz\",\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        } ],\n" +
                "        \"slop\" : 5,\n" +
                "        \"in_order\" : true,\n" +
                "        \"boost\" : 2.0\n" +
                "      }\n" +
                "    },\n" +
                "    \"little\" : {\n" +
                "      \"span_term\" : {\n" +
                "        \"field1\" : {\n" +
                "          \"value\" : \"foo\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(),
            equalTo("span_containing [big] as a nested span clause can't have non-default boost value [2.0]"));
    }

    public void testFromJsonWithNonDefaultBoostInLittleQuery() {
        String json =
                "{\n" +
                "  \"span_containing\" : {\n" +
                "    \"little\" : {\n" +
                "      \"span_near\" : {\n" +
                "        \"clauses\" : [ {\n" +
                "          \"span_term\" : {\n" +
                "            \"field1\" : {\n" +
                "              \"value\" : \"bar\",\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        }, {\n" +
                "          \"span_term\" : {\n" +
                "            \"field1\" : {\n" +
                "              \"value\" : \"baz\",\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        } ],\n" +
                "        \"slop\" : 5,\n" +
                "        \"in_order\" : true,\n" +
                "        \"boost\" : 2.0\n" +
                "      }\n" +
                "    },\n" +
                "    \"big\" : {\n" +
                "      \"span_term\" : {\n" +
                "        \"field1\" : {\n" +
                "          \"value\" : \"foo\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(),
            equalTo("span_containing [little] as a nested span clause can't have non-default boost value [2.0]"));
    }
}
