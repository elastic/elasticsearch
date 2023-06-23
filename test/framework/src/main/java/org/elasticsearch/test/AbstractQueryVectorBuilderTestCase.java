/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

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
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.QueryVectorBuilder;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

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

    // Just in case the vector builder needs to know the expected value when testing
    protected T createTestInstance(float[] expected) {
        return createTestInstance();
    }

    public final void testKnnSearchBuilderXContent() throws Exception {
        AbstractXContentTestCase.XContentTester<KnnSearchBuilder> tester = AbstractXContentTestCase.xContentTester(
            this::createParser,
            () -> new KnnSearchBuilder(randomAlphaOfLength(10), createTestInstance(), 5, 10, randomBoolean() ? null : randomFloat()),
            getToXContentParams(),
            KnnSearchBuilder::fromXContent
        );
        tester.test();
    }

    public final void testKnnSearchBuilderWireSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            KnnSearchBuilder searchBuilder = new KnnSearchBuilder(
                randomAlphaOfLength(10),
                createTestInstance(),
                5,
                10,
                randomBoolean() ? null : randomFloat()
            );
            KnnSearchBuilder serialized = copyWriteable(
                searchBuilder,
                getNamedWriteableRegistry(),
                KnnSearchBuilder::new,
                TransportVersion.current()
            );
            assertThat(serialized, equalTo(searchBuilder));
            assertNotSame(serialized, searchBuilder);
        }
    }

    public final void testKnnSearchRewrite() throws Exception {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            float[] expected = randomVector(randomIntBetween(10, 1024));
            T queryVectorBuilder = createTestInstance(expected);
            KnnSearchBuilder searchBuilder = new KnnSearchBuilder(
                randomAlphaOfLength(10),
                queryVectorBuilder,
                5,
                10,
                randomBoolean() ? null : randomFloat()
            );
            KnnSearchBuilder serialized = copyWriteable(
                searchBuilder,
                getNamedWriteableRegistry(),
                KnnSearchBuilder::new,
                TransportVersion.current()
            );
            try (NoOpClient client = new AssertingClient(expected, queryVectorBuilder)) {
                QueryRewriteContext context = new QueryRewriteContext(null, client, null);
                PlainActionFuture<KnnSearchBuilder> future = new PlainActionFuture<>();
                Rewriteable.rewriteAndFetch(randomFrom(serialized, searchBuilder), context, future);
                KnnSearchBuilder rewritten = future.get();
                assertThat(rewritten.getQueryVector(), equalTo(expected));
                assertThat(rewritten.getQueryVectorBuilder(), nullValue());
            }
        }
    }

    public final void testVectorFetch() throws Exception {
        float[] expected = randomVector(randomIntBetween(10, 1024));
        T queryVectorBuilder = createTestInstance(expected);
        try (NoOpClient client = new AssertingClient(expected, queryVectorBuilder)) {
            PlainActionFuture<float[]> future = new PlainActionFuture<>();
            queryVectorBuilder.buildVector(client, future);
            assertThat(future.get(), equalTo(expected));
        }
    }

    /**
     * Assert that the client action request is correct given this provided random builder
     * @param request The built request to be executed by the client
     * @param builder The builder used when generating this request
     */
    protected abstract void doAssertClientRequest(ActionRequest request, T builder);

    /**
     * Create a response given this expected array that is acceptable to the query builder
     * @param array The expected final array
     * @param builder The original randomly built query vector builder
     * @return An action response to be handled by the query vector builder
     */
    protected abstract ActionResponse createResponse(float[] array, T builder);

    protected static float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

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
