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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.FieldMaskingSpanQuery;
import org.apache.lucene.search.spans.SpanBoostQuery;
import org.apache.lucene.search.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.either;

public class SpanMultiTermQueryBuilderTests extends AbstractQueryTestCase<SpanMultiTermQueryBuilder> {
    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties")
            .startObject("prefix_field")
                .field("type", "text")
                .startObject("index_prefixes").endObject()
            .endObject()
            .endObject().endObject().endObject();

        mapperService.merge("_doc",
            new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

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
        assertThat(multiTermQuery, either(instanceOf(MultiTermQuery.class)).or(instanceOf(TermQuery.class)));
        assertThat(spanMultiTermQueryWrapper.getWrappedQuery(),
            equalTo(new SpanMultiTermQueryWrapper<>((MultiTermQuery)multiTermQuery).getWrappedQuery()));
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new SpanMultiTermQueryBuilder((MultiTermQueryBuilder) null));
    }

    private static class TermMultiTermQueryBuilder implements MultiTermQueryBuilder {
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
    }

    /**
     * test checks that we throw an {@link UnsupportedOperationException} if the query wrapped
     * by {@link SpanMultiTermQueryBuilder} does not generate a lucene {@link MultiTermQuery}.
     * This is currently the case for {@link RangeQueryBuilder} when the target field is mapped
     * to a date.
     */
    public void testUnsupportedInnerQueryType() throws IOException {
        MultiTermQueryBuilder query = new TermMultiTermQueryBuilder();
        SpanMultiTermQueryBuilder spamMultiTermQuery = new SpanMultiTermQueryBuilder(query);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> spamMultiTermQuery.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("unsupported inner query generated by " + TermMultiTermQueryBuilder.class.getName() +
            ", should be " + MultiTermQuery.class.getName()));
    }

    public void testToQueryInnerSpanMultiTerm() throws IOException {

        Query query = new SpanOrQueryBuilder(createTestQueryBuilder()).toQuery(createShardContext());
        //verify that the result is still a span query, despite the boost that might get set (SpanBoostQuery rather than BoostQuery)
        assertThat(query, instanceOf(SpanQuery.class));
    }

    public void testToQueryInnerTermQuery() throws IOException {
        final QueryShardContext context = createShardContext();
        if (context.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_6_4_0)) {
            Query query = new SpanMultiTermQueryBuilder(new PrefixQueryBuilder("prefix_field", "foo"))
                .toQuery(context);
            assertThat(query, instanceOf(FieldMaskingSpanQuery.class));
            FieldMaskingSpanQuery fieldSpanQuery = (FieldMaskingSpanQuery) query;
            assertThat(fieldSpanQuery.getField(), equalTo("prefix_field"));
            assertThat(fieldSpanQuery.getMaskedQuery(), instanceOf(SpanTermQuery.class));
            SpanTermQuery spanTermQuery = (SpanTermQuery) fieldSpanQuery.getMaskedQuery();
            assertThat(spanTermQuery.getTerm().text(), equalTo("foo"));

            query = new SpanMultiTermQueryBuilder(new PrefixQueryBuilder("prefix_field", "foo"))
                .boost(2.0f)
                .toQuery(context);
            assertThat(query, instanceOf(SpanBoostQuery.class));
            SpanBoostQuery boostQuery = (SpanBoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(2.0f));
            assertThat(boostQuery.getQuery(), instanceOf(FieldMaskingSpanQuery.class));
            fieldSpanQuery = (FieldMaskingSpanQuery) boostQuery.getQuery();
            assertThat(fieldSpanQuery.getField(), equalTo("prefix_field"));
            assertThat(fieldSpanQuery.getMaskedQuery(), instanceOf(SpanTermQuery.class));
            spanTermQuery = (SpanTermQuery) fieldSpanQuery.getMaskedQuery();
            assertThat(spanTermQuery.getTerm().text(), equalTo("foo"));
        } else {
            Query query = new SpanMultiTermQueryBuilder(new PrefixQueryBuilder("prefix_field", "foo"))
                .toQuery(context);
            assertThat(query, instanceOf(SpanMultiTermQueryWrapper.class));
            SpanMultiTermQueryWrapper wrapper = (SpanMultiTermQueryWrapper) query;
            assertThat(wrapper.getWrappedQuery(), instanceOf(PrefixQuery.class));
            PrefixQuery prefixQuery = (PrefixQuery) wrapper.getWrappedQuery();
            assertThat(prefixQuery.getField(), equalTo("prefix_field"));
            assertThat(prefixQuery.getPrefix().text(), equalTo("foo"));

            query = new SpanMultiTermQueryBuilder(new PrefixQueryBuilder("prefix_field", "foo"))
                .boost(2.0f)
                .toQuery(context);
            assertThat(query, instanceOf(SpanBoostQuery.class));
            SpanBoostQuery boostQuery = (SpanBoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(2.0f));
            assertThat(boostQuery.getQuery(), instanceOf(SpanMultiTermQueryWrapper.class));
            wrapper = (SpanMultiTermQueryWrapper) boostQuery.getQuery();
            assertThat(wrapper.getWrappedQuery(), instanceOf(PrefixQuery.class));
            prefixQuery = (PrefixQuery) wrapper.getWrappedQuery();
            assertThat(prefixQuery.getField(), equalTo("prefix_field"));
            assertThat(prefixQuery.getPrefix().text(), equalTo("foo"));
        }
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

    public void testDefaultMaxRewriteBuilder() throws Exception {
        Query query = QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.prefixQuery("foo", "b")).
            toQuery(createShardContext());

        if (query instanceof SpanBoostQuery) {
            query = ((SpanBoostQuery)query).getQuery();
        }

        assertTrue(query instanceof SpanMultiTermQueryWrapper);
        if (query instanceof SpanMultiTermQueryWrapper) {
            MultiTermQuery.RewriteMethod rewriteMethod = ((SpanMultiTermQueryWrapper)query).getRewriteMethod();
            assertTrue(rewriteMethod instanceof SpanMultiTermQueryBuilder.TopTermSpanBooleanQueryRewriteWithMaxClause);
        }
    }

    public void testTermExpansionExceptionOnSpanFailure() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                for (int i = 0; i < 3; i++) {
                    iw.addDocument(singleton(new TextField("body", "foo bar" + Integer.toString(i), Field.Store.NO)));
                }
                try (IndexReader reader = iw.getReader()) {
                    int origBoolMaxClauseCount = BooleanQuery.getMaxClauseCount();
                    BooleanQuery.setMaxClauseCount(1);
                    try {
                        QueryBuilder queryBuilder = new SpanMultiTermQueryBuilder(
                            QueryBuilders.prefixQuery("body", "bar")
                        );
                        Query query = queryBuilder.toQuery(createShardContext(reader));
                        RuntimeException exc = expectThrows(RuntimeException.class, () -> query.rewrite(reader));
                        assertThat(exc.getMessage(), containsString("maxClauseCount"));

                    } finally {
                        BooleanQuery.setMaxClauseCount(origBoolMaxClauseCount);
                    }
                }
            }
        }
    }

    public void testTopNMultiTermsRewriteInsideSpan() throws Exception {
        Query query = QueryBuilders.spanMultiTermQueryBuilder(
            QueryBuilders.prefixQuery("foo", "b").rewrite("top_terms_boost_2000")
        ).toQuery(createShardContext());

        if (query instanceof SpanBoostQuery) {
            query = ((SpanBoostQuery)query).getQuery();
        }

        assertTrue(query instanceof SpanMultiTermQueryWrapper);
        if (query instanceof SpanMultiTermQueryWrapper) {
            MultiTermQuery.RewriteMethod rewriteMethod = ((SpanMultiTermQueryWrapper)query).getRewriteMethod();
            assertFalse(rewriteMethod instanceof SpanMultiTermQueryBuilder.TopTermSpanBooleanQueryRewriteWithMaxClause);
        }

    }
}
