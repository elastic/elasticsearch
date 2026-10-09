/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsRequest;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IndexResolverNestedPathsTests extends ESTestCase {

    private static final QueryBuilder REQUEST_FILTER = QueryBuilders.termQuery("tier", "hot");

    private ThreadPool threadPool;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testCollectsNestedPathsOfEveryIndex() {
        FieldCapabilitiesResponse response = response(
            index(
                "idx_a",
                "hash_a",
                field("nested_punk", "nested"),
                field("nested_punk.subfield", "keyword"),
                field("obj", "object"),
                field("obj.inner", "nested"),
                field("obj.inner.leaf", "long")
            ),
            index("idx_b", "hash_b", field("other", "nested"), field("other.leaf", "keyword"), field("plain", "keyword"))
        );
        assertThat(IndexResolver.nestedPaths(response), equalTo(Set.of("nested_punk", "obj.inner", "other")));
    }

    public void testKeepsNestedPathsBelowOtherNestedPaths() {
        FieldCapabilitiesResponse response = response(
            index("idx", "hash", field("outer", "nested"), field("outer.inner", "nested"), field("outer.inner.leaf", "keyword"))
        );
        assertThat(IndexResolver.nestedPaths(response), equalTo(Set.of("outer", "outer.inner")));
    }

    public void testObjectsAndLeavesAreNotNested() {
        FieldCapabilitiesResponse response = response(
            index("idx", "hash", field("obj", "object"), field("obj.leaf", "keyword"), field("plain", "long"))
        );
        assertThat(IndexResolver.nestedPaths(response), empty());
    }

    public void testReadsEveryMappingItIsSentEvenUnderTheSameHash() {
        Map<String, IndexFieldCapabilities> shared = byName(field("nested_punk", "nested"), field("nested_punk.leaf", "keyword"));
        FieldCapabilitiesResponse response = response(
            new FieldCapabilitiesIndexResponse("idx_a", "shared", shared, true, IndexMode.STANDARD, 1, 0, 0),
            new FieldCapabilitiesIndexResponse("idx_b", "shared", shared, true, IndexMode.STANDARD, 1, 0, 0),
            index("idx_c", "shared", field("other", "nested"), field("other.leaf", "keyword"))
        );
        assertThat(IndexResolver.nestedPaths(response), equalTo(Set.of("nested_punk", "other")));
    }

    public void testReadsIndicesWithoutMappingHash() {
        FieldCapabilitiesResponse response = response(
            index("idx_a", null, field("first", "nested"), field("first.leaf", "keyword")),
            index("idx_b", null, field("second", "nested"), field("second.leaf", "keyword"))
        );
        assertThat(IndexResolver.nestedPaths(response), equalTo(Set.of("first", "second")));
    }

    public void testIgnoresIndicesThatCannotMatch() {
        FieldCapabilitiesResponse response = response(index("idx", "hash", false, field("punk", "nested"), field("punk.leaf", "keyword")));
        assertThat(IndexResolver.nestedPaths(response), empty());
    }

    public void testNestedPathsRequestKeepsNestedFieldsOfTheMainRequest() {
        QueryBuilder filter = QueryBuilders.termQuery("tier", "hot");
        FieldCapabilitiesRequest request = IndexResolver.createNestedPathsRequest(
            IndexResolver.DEFAULT_OPTIONS,
            "logs-*,remote:metrics",
            null,
            filter,
            false
        ).fieldCapsRequest();
        assertThat(request.indices(), arrayContaining("logs-*", "remote:metrics"));
        assertThat(request.fields(), arrayContaining("*"));
        assertThat(request.filters(), arrayContainingInAnyOrder("-metadata", "-multifield"));
        assertThat(request.indexFilter(), equalTo(filter));
        assertThat(request.indicesOptions(), equalTo(IndexResolver.DEFAULT_OPTIONS));
        assertThat(request.isMergeResults(), is(false));
        assertThat(request.includeEmptyFields(), is(true));
        assertThat(request.includeResolvedTo(), is(false));
    }

    public void testNestedPathsRequestKeepsCrossProjectOptionsAndRouting() {
        IndicesOptions strict = IndicesOptions.builder(IndexResolver.DEFAULT_OPTIONS)
            .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS)
            .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
            .build();
        FieldCapabilitiesRequest request = IndexResolver.createNestedPathsRequest(strict, "logs-*", "_alias:linked", null, true)
            .fieldCapsRequest();
        assertThat(request.indicesOptions(), equalTo(strict));
        assertThat(request.includeResolvedTo(), is(true));
        assertThat(request.getProjectRouting(), equalTo("_alias:linked"));
    }

    public void testWithoutNestedPathsOnlyTheMainRequestIsSent() {
        FieldCapsClient client = new FieldCapsClient(threadPool, request -> response(index("idx", "hash", field("id", "keyword"))));
        EsIndex index = resolve(client, false).get();
        assertThat(client.requests, hasSize(1));
        assertThat(Arrays.asList(client.requests.getFirst().filters()), equalTo(List.of("-nested")));
        assertThat(index.nestedPaths(), empty());
    }

    public void testNestedPathsRequestRunsAlongsideTheMainOne() {
        FieldCapsClient client = new FieldCapsClient(
            threadPool,
            request -> isMain(request)
                ? response(index("nested_idx", "hash_n", field("id", "keyword")), index("missing_idx", "hash_m", field("id", "keyword")))
                : response(
                    index("nested_idx", "hash_n", field("id", "keyword"), field("punk", "nested"), field("punk.leaf", "keyword")),
                    index("missing_idx", "hash_m", field("id", "keyword"))
                )
        );
        EsIndex index = resolve(client, true).get();
        assertThat(client.requests, hasSize(2));
        assertThat(client.requests.stream().map(FieldCapabilitiesRequest::indexFilter).toList(), everyItem(equalTo(REQUEST_FILTER)));
        assertThat(index.nestedPaths(), equalTo(Set.of("punk")));
        assertThat(index.mapping().keySet(), equalTo(Set.of("id")));
    }

    public void testFailureWithoutAMessageFailsTheResolution() {
        FieldCapsClient client = new FieldCapsClient(threadPool, request -> {
            if (isMain(request)) {
                return response(index("idx", "hash", field("id", "keyword")));
            }
            throw new IllegalStateException();
        });
        PlainActionFuture<Versioned<IndexResolution>> future = resolveAsync(client, true);
        assertTrue(future.isDone());
        expectThrows(IllegalStateException.class, future::actionGet);
    }

    public void testUnavailableRemoteFailingTheWholeNestedPathsRequestFailsTheResolution() {
        FieldCapsClient client = new FieldCapsClient(threadPool, request -> {
            if (isMain(request)) {
                return response(index("r1:logs-1", "hash_r1", field("id", "keyword")));
            }
            throw new NoSuchRemoteClusterException("r1");
        });
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> resolve(client, true));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertFalse(ExceptionsHelper.isRemoteUnavailableException(e));
        assertThat(e.getSuppressed()[0], instanceOf(NoSuchRemoteClusterException.class));
    }

    public void testMainResponseArrivingFirstWaitsForTheNestedPaths() {
        FieldCapsClient client = new FieldCapsClient(
            threadPool,
            request -> isMain(request)
                ? response(index("idx", "hash", field("id", "keyword")))
                : response(index("idx", "hash", field("id", "keyword"), field("punk", "nested"), field("punk.leaf", "keyword")))
        );
        client.deferNestedPaths = true;
        PlainActionFuture<Versioned<IndexResolution>> future = resolveAsync(client, true);
        assertThat(client.requests, hasSize(2));
        assertFalse(future.isDone());
        client.answerDeferred();
        assertThat(future.actionGet().inner().get().nestedPaths(), equalTo(Set.of("punk")));
    }

    public void testFailingNestedPathsRequestFailsTheResolution() {
        FieldCapsClient client = new FieldCapsClient(threadPool, request -> {
            if (isMain(request)) {
                return response(index("idx", "hash", field("id", "keyword")));
            }
            throw new IllegalArgumentException("simulated nested paths failure");
        });
        Exception e = expectThrows(IllegalArgumentException.class, () -> resolve(client, true));
        assertThat(e.getMessage(), equalTo("simulated nested paths failure"));
    }

    private IndexResolution resolve(FieldCapsClient client, boolean resolveNestedPaths) {
        return resolveAsync(client, resolveNestedPaths).actionGet().inner();
    }

    private PlainActionFuture<Versioned<IndexResolution>> resolveAsync(FieldCapsClient client, boolean resolveNestedPaths) {
        PlainActionFuture<Versioned<IndexResolution>> future = new PlainActionFuture<>();
        new IndexResolver(client, () -> true).resolveMainIndicesVersioned(
            "*",
            IndexResolver.ALL_FIELDS,
            REQUEST_FILTER,
            false,
            TransportVersion.current(),
            false,
            false,
            false,
            false,
            true,
            resolveNestedPaths,
            (options, expressions, returnLocalAll) -> Map.of(
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                new OriginalIndices(expressions, options)
            ),
            future
        );
        return future;
    }

    private static boolean isMain(FieldCapabilitiesRequest request) {
        return Arrays.asList(request.filters()).contains("-nested");
    }

    /**
     * Stands in for the field caps action, which needs a cluster: answers each request from {@code responder}, at once or, for the
     * nested paths requests with {@link #deferNestedPaths}, on {@link #answerDeferred}, and records them.
     */
    private static class FieldCapsClient extends NoOpClient {
        private final Function<FieldCapabilitiesRequest, FieldCapabilitiesResponse> responder;
        private final List<FieldCapabilitiesRequest> requests = new ArrayList<>();
        private final List<Runnable> deferred = new ArrayList<>();
        private boolean deferNestedPaths;

        FieldCapsClient(ThreadPool threadPool, Function<FieldCapabilitiesRequest, FieldCapabilitiesResponse> responder) {
            super(threadPool);
            this.responder = responder;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(EsqlResolveFieldsAction.TYPE));
            FieldCapabilitiesRequest fieldCapsRequest = ((EsqlResolveFieldsRequest) request).fieldCapsRequest();
            requests.add(fieldCapsRequest);
            Runnable answer = () -> ActionListener.completeWith(listener, () -> {
                // The action above is EsqlResolveFieldsAction.TYPE, whose response type is EsqlResolveFieldsResponse.
                @SuppressWarnings("unchecked")
                Response response = (Response) new EsqlResolveFieldsResponse(responder.apply(fieldCapsRequest));
                return response;
            });
            if (deferNestedPaths && isMain(fieldCapsRequest) == false) {
                deferred.add(answer);
            } else {
                answer.run();
            }
        }

        void answerDeferred() {
            List<Runnable> answers = List.copyOf(deferred);
            deferred.clear();
            answers.forEach(Runnable::run);
        }
    }

    private static FieldCapabilitiesResponse response(FieldCapabilitiesIndexResponse... indices) {
        return new FieldCapabilitiesResponse(new ArrayList<>(Arrays.asList(indices)), List.of());
    }

    private static FieldCapabilitiesIndexResponse index(String name, String mappingHash, IndexFieldCapabilities... fields) {
        return index(name, mappingHash, true, fields);
    }

    private static FieldCapabilitiesIndexResponse index(
        String name,
        String mappingHash,
        boolean canMatch,
        IndexFieldCapabilities... fields
    ) {
        return new FieldCapabilitiesIndexResponse(name, mappingHash, byName(fields), canMatch, IndexMode.STANDARD, 1, 0, 0);
    }

    private static Map<String, IndexFieldCapabilities> byName(IndexFieldCapabilities... fields) {
        return Arrays.stream(fields).collect(Collectors.toMap(IndexFieldCapabilities::name, Function.identity()));
    }

    private static IndexFieldCapabilities field(String name, String type) {
        return new IndexFieldCapabilitiesBuilder(name, type).build();
    }
}
