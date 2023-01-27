/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.vectors.KnnSearchBuilderTests.randomVector;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests a query vector builder
 * @param <T> the query vector builder type to test
 */
public abstract class AbstractQueryVectorBuilderTestCase<T extends QueryVectorBuilder> extends AbstractXContentSerializingTestCase<T> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    protected List<SearchPlugin> additionalPlugins() {
        return List.of();
    }

    @Before
    public void registerNamedXContents() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, additionalPlugins());
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    public void testKnnSearchBuilderXContent() throws Exception {
        AbstractXContentTestCase.XContentTester<KnnSearchBuilder> tester = AbstractXContentTestCase.xContentTester(
            this::createParser,
            () -> new KnnSearchBuilder(randomAlphaOfLength(10), createTestInstance(), 5, 10),
            getToXContentParams(),
            KnnSearchBuilder::fromXContent
        );
        tester.test();
    }

    public void testKnnSearchBuilderWireSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            KnnSearchBuilder searchBuilder = new KnnSearchBuilder(randomAlphaOfLength(10), createTestInstance(), 5, 10);
            KnnSearchBuilder serialized = copyWriteable(
                searchBuilder,
                getNamedWriteableRegistry(),
                KnnSearchBuilder::new,
                TransportVersion.CURRENT
            );
            assertThat(serialized, equalTo(searchBuilder));
            assertNotSame(serialized, searchBuilder);
        }
    }

    public void testKnnSearchRewrite() throws Exception {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            T queryVectorBuilder = createTestInstance();
            KnnSearchBuilder searchBuilder = new KnnSearchBuilder(randomAlphaOfLength(10), queryVectorBuilder, 5, 10);
            KnnSearchBuilder serialized = copyWriteable(
                searchBuilder,
                getNamedWriteableRegistry(),
                KnnSearchBuilder::new,
                TransportVersion.CURRENT
            );
            float[] expected = randomVector(randomIntBetween(10, 1024));
            try (NoOpClient client = new AssertingClient(expected, queryVectorBuilder)) {
                QueryRewriteContext context = new QueryRewriteContext(null, null, client, null);
                PlainActionFuture<KnnSearchBuilder> future = new PlainActionFuture<>();
                Rewriteable.rewriteAndFetch(randomFrom(serialized, searchBuilder), context, future);
                KnnSearchBuilder rewritten = future.get();
                assertThat(rewritten.queryVector, equalTo(expected));
                assertThat(rewritten.queryVectorBuilder, nullValue());
            }
        }
    }

    public void testVectorFetch() throws Exception {
        T queryVectorBuilder = createTestInstance();
        float[] expected = randomVector(randomIntBetween(10, 1024));
        try (NoOpClient client = new AssertingClient(expected, queryVectorBuilder)) {
            PlainActionFuture<float[]> future = new PlainActionFuture<>();
            queryVectorBuilder.buildVector(client, future);
            assertThat(future.get(), equalTo(expected));
        }
    }

    abstract void doAssertClientRequest(ActionRequest request, T builder);

    abstract ActionResponse createResponse(float[] array, T builder);

    private class AssertingClient extends NoOpClient {

        private final float[] array;
        private final T queryVectorBuilder;

        AssertingClient(float[] array, T queryVectorBuilder) {
            super("query_vector_builder_tests");
            this.array = array;
            this.queryVectorBuilder = queryVectorBuilder;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            doAssertClientRequest(request, queryVectorBuilder);
            listener.onResponse((Response) createResponse(array, queryVectorBuilder));
        }
    }
}
