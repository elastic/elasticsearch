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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanMultiTermQueryBuilderTests extends AbstractQueryTestCase<SpanMultiTermQueryBuilder> {
    @Override
    protected SpanMultiTermQueryBuilder doCreateTestQueryBuilder() {
        MultiTermQueryBuilder multiTermQueryBuilder = RandomQueryBuilder.createMultiTermQuery(random());
        return new SpanMultiTermQueryBuilder(multiTermQueryBuilder);
    }

    @Override
    protected void doAssertLuceneQuery(SpanMultiTermQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        if (queryBuilder.innerQuery().boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            assertThat(query, instanceOf(SpanBoostQuery.class));
            SpanBoostQuery boostQuery = (SpanBoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(queryBuilder.innerQuery().boost()));
            query = boostQuery.getQuery();
        }
        assertThat(query, instanceOf(SpanMultiTermQueryWrapper.class));
        SpanMultiTermQueryWrapper spanMultiTermQueryWrapper = (SpanMultiTermQueryWrapper) query;
        Query multiTermQuery = queryBuilder.innerQuery().toQuery(context.getQueryShardContext());
        if (queryBuilder.innerQuery().boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            assertThat(multiTermQuery, instanceOf(BoostQuery.class));
            BoostQuery boostQuery = (BoostQuery) multiTermQuery;
            multiTermQuery = boostQuery.getQuery();
        }
        assertThat(multiTermQuery, instanceOf(MultiTermQuery.class));
        assertThat(spanMultiTermQueryWrapper.getWrappedQuery(), equalTo(new SpanMultiTermQueryWrapper<>((MultiTermQuery)multiTermQuery).getWrappedQuery()));
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new SpanMultiTermQueryBuilder((MultiTermQueryBuilder) null));
    }

    /**
     * test checks that we throw an {@link UnsupportedOperationException} if the query wrapped
     * by {@link SpanMultiTermQueryBuilder} does not generate a lucene {@link MultiTermQuery}.
     * This is currently the case for {@link RangeQueryBuilder} when the target field is mapped
     * to a date.
     */
    public void testUnsupportedInnerQueryType() throws IOException {
        MultiTermQueryBuilder query = new MultiTermQueryBuilder() {
            @Override
            public Query toQuery(QueryShardContext context) throws IOException {
                return new TermQuery(new Term("foo", "bar"));
            }

            @Override
            public Query toFilter(QueryShardContext context) throws IOException {
                return toQuery(context);
            }

            @Override
            public QueryBuilder queryName(String queryName) {
                return this;
            }

            @Override
            public String queryName() {
                return "foo";
            }

            @Override
            public float boost() {
                return 1f;
            }

            @Override
            public QueryBuilder boost(float boost) {
                return this;
            }

            @Override
            public String getName() {
                return "foo";
            }

            @Override
            public String getWriteableName() {
                return "foo";
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {

            }
        };
        SpanMultiTermQueryBuilder spamMultiTermQuery = new SpanMultiTermQueryBuilder(query);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> spamMultiTermQuery.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("unsupported inner query, should be " + MultiTermQuery.class.getName()));
    }

    public void testToQueryInnerSpanMultiTerm() throws IOException {
        Query query = new SpanOrQueryBuilder(createTestQueryBuilder()).toQuery(createShardContext());
        //verify that the result is still a span query, despite the boost that might get set (SpanBoostQuery rather than BoostQuery)
        assertThat(query, instanceOf(SpanQuery.class));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"span_multi\" : {\n" +
                "    \"match\" : {\n" +
                "      \"prefix\" : {\n" +
                "        \"user\" : {\n" +
                "          \"value\" : \"ki\",\n" +
                "          \"boost\" : 1.08\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        SpanMultiTermQueryBuilder parsed = (SpanMultiTermQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "ki", ((PrefixQueryBuilder) parsed.innerQuery()).value());
        assertEquals(json, 1.08, parsed.innerQuery().boost(), 0.0001);
    }
}
