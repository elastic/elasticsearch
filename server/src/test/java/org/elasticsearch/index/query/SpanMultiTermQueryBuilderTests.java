/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.SpanMatchNoDocsQuery;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.SpanBooleanQueryRewriteWithMaxClause;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static java.util.Collections.singleton;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;

public class SpanMultiTermQueryBuilderTests extends AbstractQueryTestCase<SpanMultiTermQueryBuilder> {

    @Override
    protected boolean supportsBoost() {
        return false;
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties")
            .startObject("prefix_field")
                .field("type", "text")
                .startObject("index_prefixes").endObject()
            .endObject()
            .startObject("prefix_field_alias")
                .field("type", "alias")
                .field("path", "prefix_field")
            .endObject()
            .startObject("body")
                .field("type", "text")
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
    protected void doAssertLuceneQuery(SpanMultiTermQueryBuilder queryBuilder, Query query,
                                       SearchExecutionContext context) throws IOException {
        if (query instanceof SpanMatchNoDocsQuery) {
            return;
        }
        assertThat(query, either(instanceOf(SpanMultiTermQueryWrapper.class))
                .or(instanceOf(FieldMaskingSpanQuery.class)));
        if (query instanceof SpanMultiTermQueryWrapper) {
            SpanMultiTermQueryWrapper<?> wrapper = (SpanMultiTermQueryWrapper<?>) query;
            Query innerQuery = queryBuilder.innerQuery().toQuery(context);
            if (queryBuilder.innerQuery().boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
                assertThat(innerQuery, instanceOf(BoostQuery.class));
                BoostQuery boostQuery = (BoostQuery) innerQuery;
                innerQuery = boostQuery.getQuery();
            }
            assertThat(innerQuery, instanceOf(MultiTermQuery.class));
            MultiTermQuery multiQuery = (MultiTermQuery) innerQuery;
            if (multiQuery.getRewriteMethod() instanceof TopTermsRewrite) {
                assertThat(wrapper.getRewriteMethod(), instanceOf(SpanMultiTermQueryWrapper.TopTermsSpanBooleanQueryRewrite.class));
            } else {
                assertThat(wrapper.getRewriteMethod(), instanceOf(SpanBooleanQueryRewriteWithMaxClause.class));
            }
        } else if (query instanceof FieldMaskingSpanQuery) {
            FieldMaskingSpanQuery mask = (FieldMaskingSpanQuery) query;
            assertThat(mask.getMaskedQuery(), instanceOf(TermQuery.class));
        }
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new SpanMultiTermQueryBuilder((MultiTermQueryBuilder) null));
    }

    private static class TermMultiTermQueryBuilder implements MultiTermQueryBuilder {
        @Override
        public Query toQuery(SearchExecutionContext context) throws IOException {
            return new TermQuery(new Term("foo", "bar"));
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

        @Override
        public String fieldName() {
            return "foo";
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
        SpanMultiTermQueryBuilder spanMultiTermQuery = new SpanMultiTermQueryBuilder(query);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> spanMultiTermQuery.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), startsWith("unsupported inner query"));
    }

    public void testToQueryInnerSpanMultiTerm() throws IOException {
        Query query = new SpanOrQueryBuilder(createTestQueryBuilder()).toQuery(createSearchExecutionContext());
        //verify that the result is still a span query, despite the boost that might get set (SpanBoostQuery rather than BoostQuery)
        assertThat(query, instanceOf(SpanQuery.class));
    }

    public void testToQueryInnerTermQuery() throws IOException {
        String fieldName = randomFrom("prefix_field", "prefix_field_alias");
        final SearchExecutionContext context = createSearchExecutionContext();
        {
            Query query = new SpanMultiTermQueryBuilder(new PrefixQueryBuilder(fieldName, "foo")).toQuery(context);
            assertThat(query, instanceOf(FieldMaskingSpanQuery.class));
            FieldMaskingSpanQuery fieldQuery = (FieldMaskingSpanQuery) query;
            assertThat(fieldQuery.getMaskedQuery(), instanceOf(SpanTermQuery.class));
            assertThat(fieldQuery.getField(), equalTo("prefix_field"));
            SpanTermQuery termQuery = (SpanTermQuery) fieldQuery.getMaskedQuery();
            assertThat(termQuery.getTerm().field(), equalTo("prefix_field._index_prefix"));
            assertThat(termQuery.getTerm().text(), equalTo("foo"));
        }

        {
            Query query = new SpanMultiTermQueryBuilder(new PrefixQueryBuilder(fieldName, "f")).toQuery(context);
            assertThat(query, instanceOf(SpanMultiTermQueryWrapper.class));
            SpanMultiTermQueryWrapper<?> wrapper = (SpanMultiTermQueryWrapper<?>) query;
            assertThat(wrapper.getWrappedQuery(), instanceOf(PrefixQuery.class));
            assertThat(wrapper.getField(), equalTo("prefix_field"));
            PrefixQuery prefixQuery = (PrefixQuery) wrapper.getWrappedQuery();
            assertThat(prefixQuery.getField(), equalTo("prefix_field"));
            assertThat(prefixQuery.getPrefix().text(), equalTo("f"));
            assertThat(wrapper.getRewriteMethod(), instanceOf(SpanBooleanQueryRewriteWithMaxClause.class));
            SpanBooleanQueryRewriteWithMaxClause rewrite = (SpanBooleanQueryRewriteWithMaxClause) wrapper.getRewriteMethod();
            assertThat(rewrite.getMaxExpansions(), equalTo(BooleanQuery.getMaxClauseCount()));
            assertTrue(rewrite.isHardLimit());
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
        Query query = QueryBuilders.spanMultiTermQueryBuilder(QueryBuilders.prefixQuery("body", "b"))
            .toQuery(createSearchExecutionContext());

        assertTrue(query instanceof SpanMultiTermQueryWrapper);
        if (query instanceof SpanMultiTermQueryWrapper) {
            MultiTermQuery.RewriteMethod rewriteMethod = ((SpanMultiTermQueryWrapper) query).getRewriteMethod();
            assertTrue(rewriteMethod instanceof SpanBooleanQueryRewriteWithMaxClause);
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
                        Query query = queryBuilder.toQuery(createSearchExecutionContext(new IndexSearcher(reader)));
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
            QueryBuilders.prefixQuery("body", "b").rewrite("top_terms_boost_2000")
        ).toQuery(createSearchExecutionContext());

        assertTrue(query instanceof SpanMultiTermQueryWrapper);
        if (query instanceof SpanMultiTermQueryWrapper) {
            MultiTermQuery.RewriteMethod rewriteMethod = ((SpanMultiTermQueryWrapper)query).getRewriteMethod();
            assertFalse(rewriteMethod instanceof SpanBooleanQueryRewriteWithMaxClause);
        }

    }
}
