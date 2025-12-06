/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class QueryRewriteContextIT extends ESIntegTestCase {
    private static class TestQueryBuilder extends AbstractQueryBuilder<TestQueryBuilder> {
        private static final String NAME = "test";

        private static TestQueryBuilder fromXContent(XContentParser parser) {
            return new TestQueryBuilder();
        }

        TestQueryBuilder() {}

        TestQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {

        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.endObject();
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) throws IOException {
            return new MatchNoDocsQuery();
        }

        @Override
        protected boolean doEquals(TestQueryBuilder other) {
            return true;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }
    }

    public static class TestPlugin extends Plugin implements SearchPlugin {
        public TestPlugin() {}

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<QueryBuilder>(TestQueryBuilder.NAME, TestQueryBuilder::new, TestQueryBuilder::fromXContent));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    public void testResolvedIndices_TransportSearchAction() {
        final String[] indices = { "test1", "test2" };
        createIndex(indices);

        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(indices, "alias"));
        assertResolvedIndices(prepareSearch(indices), Set.of(indices), Set.of(indices), r -> {});
        assertResolvedIndices(prepareSearch("test*"), Set.of("test*"), Set.of(indices), r -> {});
        assertResolvedIndices(prepareSearch("alias"), Set.of("alias"), Set.of(indices), r -> {});

        final BytesReference pointInTimeId = openPointInTime(indices, TimeValue.timeValueMinutes(2));
        try {
            final PointInTimeBuilder pointInTimeBuilder = new PointInTimeBuilder(pointInTimeId);
            assertResolvedIndices(prepareSearch().setPointInTime(pointInTimeBuilder), Set.of(indices), Set.of(indices), r -> {});

            assertAcked(indicesAdmin().prepareDelete("test2"));
            assertResolvedIndices(prepareSearch().setPointInTime(pointInTimeBuilder), Set.of(indices), Set.of("test1"), r -> {});
        } finally {
            closePointInTime(pointInTimeId);
        }

    }

    public void testResolvedIndices_TransportExplainAction() {
        final String[] indices = { "test1", "test2" };
        createIndex(indices);
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("test1", "alias1"),
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(indices, "alias2")
        );

        assertResolvedIndices(client().prepareExplain("test1", "1"), Set.of("test1"), Set.of("test1"), r -> {});
        assertResolvedIndices(client().prepareExplain("alias1", "1"), Set.of("alias1"), Set.of("test1"), r -> {});
        assertRequestBuilderThrows(client().prepareExplain("alias2", "1"), IllegalArgumentException.class, RestStatus.BAD_REQUEST);
    }

    public void testResolvedIndices_TransportValidateQueryAction() {
        final String[] indices = { "test1", "test2" };
        createIndex(indices);
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(indices, "alias"));

        Consumer<ValidateQueryResponse> responseAssertions = r -> {
            assertThat(r.getStatus(), equalTo(RestStatus.OK));
            assertThat(r.isValid(), is(true));
        };

        assertResolvedIndices(
            client().admin().indices().prepareValidateQuery(indices),
            Set.of(indices),
            Set.of(indices),
            responseAssertions
        );
        assertResolvedIndices(
            client().admin().indices().prepareValidateQuery("test*"),
            Set.of("test*"),
            Set.of(indices),
            responseAssertions
        );
        assertResolvedIndices(
            client().admin().indices().prepareValidateQuery("alias"),
            Set.of("alias"),
            Set.of(indices),
            responseAssertions
        );
    }

    private BytesReference openPointInTime(String[] indices, TimeValue keepAlive) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        OpenPointInTimeResponse response = client().execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        return response.getPointInTimeId();
    }

    private void closePointInTime(BytesReference pointInTimeId) {
        ClosePointInTimeResponse response = client().execute(
            TransportClosePointInTimeAction.TYPE,
            new ClosePointInTimeRequest(pointInTimeId)
        ).actionGet();
        assertThat(response.status(), is(RestStatus.OK));
    }

    private static <Request extends ActionRequest, Response extends ActionResponse> void assertResolvedIndices(
        ActionRequestBuilder<Request, Response> requestBuilder,
        @Nullable Set<String> expectedLocalIndices,
        Set<String> expectedConcreteLocalIndices,
        Consumer<Response> responseAssertions
    ) {
        AtomicBoolean gotQueryRewriteContext = new AtomicBoolean(false);
        TestQueryBuilder testQueryBuilder = new TestQueryBuilder() {
            @Override
            protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
                // Check that the first QueryRewriteContext received has the expected resolved indices.
                // Later QueryRewriteContext instances received, such as the one generated in the can-match phase, will have resolved
                // indices set to null.
                if (queryRewriteContext.getClass() == QueryRewriteContext.class && gotQueryRewriteContext.getAndSet(true) == false) {
                    ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
                    assertThat(resolvedIndices, notNullValue());

                    OriginalIndices localIndices = resolvedIndices.getLocalIndices();
                    if (expectedLocalIndices != null) {
                        assertThat(localIndices, notNullValue());
                        assertThat(Set.of(localIndices.indices()), equalTo(expectedLocalIndices));
                    } else {
                        assertThat(localIndices, nullValue());
                    }

                    assertThat(
                        Arrays.stream(resolvedIndices.getConcreteLocalIndices()).map(Index::getName).collect(Collectors.toSet()),
                        equalTo(expectedConcreteLocalIndices)
                    );

                    Map<Index, IndexMetadata> indexMetadataMap = resolvedIndices.getConcreteLocalIndicesMetadata();
                    assertThat(indexMetadataMap.size(), equalTo(expectedConcreteLocalIndices.size()));
                    indexMetadataMap.forEach((k, v) -> {
                        assertThat(expectedConcreteLocalIndices.contains(k.getName()), is(true));
                        assertThat(v, notNullValue());
                    });
                }

                return super.doRewrite(queryRewriteContext);
            }
        };

        setQuery(requestBuilder, testQueryBuilder);
        assertResponse(requestBuilder, responseAssertions);
        assertThat(gotQueryRewriteContext.get(), is(true));
    }

    private static <Request extends ActionRequest, Response extends ActionResponse> void setQuery(
        ActionRequestBuilder<Request, Response> requestBuilder,
        QueryBuilder queryBuilder
    ) {
        if (requestBuilder instanceof SearchRequestBuilder searchRequestBuilder) {
            searchRequestBuilder.setQuery(queryBuilder);
        } else if (requestBuilder instanceof ExplainRequestBuilder explainRequestBuilder) {
            explainRequestBuilder.setQuery(queryBuilder);
        } else if (requestBuilder instanceof ValidateQueryRequestBuilder validateQueryRequestBuilder) {
            validateQueryRequestBuilder.setQuery(queryBuilder);
        } else {
            throw new AssertionError("Unexpected request builder type [" + requestBuilder.getClass() + "]");
        }
    }
}
