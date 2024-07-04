/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.hamcrest.Matchers.equalTo;

public class RetrieverRewriteIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AssertingPlugin.class);
    }

    private static String INDEX_DOCS = "docs";
    private static String INDEX_QUERIES = "queries";
    private static final String ID_FIELD = "_id";
    private static final String QUERY_FIELD = "query";

    @Before
    public void setup() throws Exception {
        createIndex(INDEX_DOCS);
        index(INDEX_DOCS, "doc_0", "{}");
        index(INDEX_DOCS, "doc_1", "{}");
        index(INDEX_DOCS, "doc_2", "{}");
        refresh(INDEX_DOCS);

        createIndex(INDEX_QUERIES);
        index(INDEX_QUERIES, "query_0", "{ \"" + QUERY_FIELD + "\": \"doc_2\"}");
        index(INDEX_QUERIES, "query_1", "{ \"" + QUERY_FIELD + "\": \"doc_1\"}");
        index(INDEX_QUERIES, "query_2", "{ \"" + QUERY_FIELD + "\": \"doc_0\"}");
        refresh(INDEX_QUERIES);
    }

    public void testRewrite() throws ExecutionException, InterruptedException {
        SearchSourceBuilder source = new SearchSourceBuilder();
        StandardRetrieverBuilder standard = new StandardRetrieverBuilder();
        standard.queryBuilder = QueryBuilders.termQuery(ID_FIELD, "doc_0");
        source.retriever(new AssertingRetrieverBuilder(standard));
        SearchRequest req = new SearchRequest(INDEX_DOCS, INDEX_QUERIES).source(source);
        SearchResponse resp = client().search(req).get();
        assertNull(resp.pointInTimeId());
        assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
        assertThat(resp.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
        assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_0"));
    }

    public void testRewriteCompound() throws ExecutionException, InterruptedException {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.retriever(new AssertingCompoundRetrieverBuilder("query_0"));
        SearchRequest req = new SearchRequest(INDEX_DOCS, INDEX_QUERIES).source(source);
        SearchResponse resp = client().search(req).get();
        assertNull(resp.pointInTimeId());
        assertThat(resp.getHits().getTotalHits().value, equalTo(1L));
        assertThat(resp.getHits().getTotalHits().relation, equalTo(TotalHits.Relation.EQUAL_TO));
        assertThat(resp.getHits().getAt(0).getId(), equalTo("doc_2"));
    }

    public static class AssertingPlugin extends Plugin implements SearchPlugin {
        public AssertingPlugin() {}

        @Override
        public List<RetrieverSpec<?>> getRetrievers() {
            return List.of(
                new RetrieverSpec<RetrieverBuilder>(AssertingRetrieverBuilder.NAME, AssertingRetrieverBuilder::fromXContent),
                new RetrieverSpec<RetrieverBuilder>(AssertingCompoundRetrieverBuilder.NAME, AssertingCompoundRetrieverBuilder::fromXContent)
            );
        }
    }

    public static final ConstructingObjectParser<AssertingRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        AssertingCompoundRetrieverBuilder.NAME,
        args -> new AssertingRetrieverBuilder((RetrieverBuilder) args[0])
    );

    public static final ConstructingObjectParser<AssertingCompoundRetrieverBuilder, RetrieverParserContext> PARSER_COMPOUND =
        new ConstructingObjectParser<>(
            AssertingCompoundRetrieverBuilder.NAME,
            args -> new AssertingCompoundRetrieverBuilder((String) args[0])
        );

    static {
        RetrieverBuilder.declareBaseParserFields(AssertingRetrieverBuilder.NAME, PARSER);
        PARSER.declareObject(constructorArg(), RetrieverBuilder::parseInnerRetrieverBuilder, new ParseField("retriever"));

        RetrieverBuilder.declareBaseParserFields(AssertingCompoundRetrieverBuilder.NAME, PARSER_COMPOUND);
        PARSER_COMPOUND.declareString(constructorArg(), new ParseField("id"));
    }

    private static class AssertingRetrieverBuilder extends RetrieverBuilder {
        static final String NAME = "asserting";

        private final RetrieverBuilder innerRetriever;

        public static AssertingRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
            return PARSER.apply(parser, context);
        }

        private AssertingRetrieverBuilder(RetrieverBuilder innerRetriever) {
            this.innerRetriever = innerRetriever;
        }

        @Override
        public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
            assertNull(ctx.getPointInTimeBuilder());
            assertNull(ctx.convertToInnerHitsRewriteContext());
            assertNull(ctx.convertToCoordinatorRewriteContext());
            assertNull(ctx.convertToIndexMetadataContext());
            assertNull(ctx.convertToSearchExecutionContext());
            assertNull(ctx.convertToDataRewriteContext());
            var newRetriever = innerRetriever.rewrite(ctx);
            if (newRetriever != innerRetriever) {
                return new AssertingRetrieverBuilder(newRetriever);
            }
            return this;
        }

        @Override
        public void extractToSearchSourceBuilder(SearchSourceBuilder sourceBuilder, boolean compoundUsed) {
            assertNull(sourceBuilder.retriever());
            innerRetriever.extractToSearchSourceBuilder(sourceBuilder, compoundUsed);
        }

        @Override
        public String getName() {
            return "asserting";
        }

        @Override
        protected void doToXContent(XContentBuilder builder, Params params) throws IOException {}

        @Override
        protected boolean doEquals(Object o) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return innerRetriever.doHashCode();
        }
    }

    private static class AssertingCompoundRetrieverBuilder extends RetrieverBuilder {
        static final String NAME = "asserting_compound";

        private final String id;
        private final SetOnce<RetrieverBuilder> innerRetriever;

        public static AssertingCompoundRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context)
            throws IOException {
            return PARSER_COMPOUND.apply(parser, context);
        }

        private AssertingCompoundRetrieverBuilder(String id) {
            this.id = id;
            this.innerRetriever = new SetOnce<>(null);
        }

        private AssertingCompoundRetrieverBuilder(String id, SetOnce<RetrieverBuilder> innerRetriever) {
            this.id = id;
            this.innerRetriever = innerRetriever;
        }

        @Override
        public boolean isCompound() {
            return true;
        }

        @Override
        public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
            assertNotNull(ctx.getPointInTimeBuilder());
            assertNull(ctx.convertToInnerHitsRewriteContext());
            assertNull(ctx.convertToCoordinatorRewriteContext());
            assertNull(ctx.convertToIndexMetadataContext());
            assertNull(ctx.convertToSearchExecutionContext());
            assertNull(ctx.convertToDataRewriteContext());
            if (innerRetriever.get() != null) {
                return this;
            }
            SetOnce<RetrieverBuilder> innerRetriever = new SetOnce<>();
            ctx.registerAsyncAction((client, actionListener) -> {
                SearchSourceBuilder source = new SearchSourceBuilder().pointInTimeBuilder(ctx.getPointInTimeBuilder())
                    .query(QueryBuilders.termQuery(ID_FIELD, id))
                    .fetchField(QUERY_FIELD);
                client.search(new SearchRequest().source(source), new ActionListener<>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        String query = response.getHits().getAt(0).field(QUERY_FIELD).getValue();
                        StandardRetrieverBuilder standard = new StandardRetrieverBuilder();
                        standard.queryBuilder = QueryBuilders.termQuery(ID_FIELD, query);
                        innerRetriever.set(standard);
                        actionListener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        actionListener.onFailure(e);
                    }
                });
            });
            return new AssertingCompoundRetrieverBuilder(id, innerRetriever);
        }

        @Override
        public void extractToSearchSourceBuilder(SearchSourceBuilder sourceBuilder, boolean compoundUsed) {
            assertNull(sourceBuilder.retriever());
            innerRetriever.get().extractToSearchSourceBuilder(sourceBuilder, compoundUsed);
        }

        @Override
        public String getName() {
            return "asserting";
        }

        @Override
        protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
            throw new AssertionError("not implemented");
        }

        @Override
        protected boolean doEquals(Object o) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return id.hashCode();
        }
    }
}
