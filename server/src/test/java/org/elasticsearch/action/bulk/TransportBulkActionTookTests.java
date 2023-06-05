/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class TransportBulkActionTookTests extends ESTestCase {

    private static ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportBulkActionTookTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create(
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            VersionUtils.randomCompatibleVersion(random(), Version.CURRENT)
        );
        clusterService = createClusterService(threadPool, discoveryNode);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    private TransportBulkAction createAction(boolean controlled, AtomicLong expected) {
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        IndexNameExpressionResolver resolver = new Resolver();
        ActionFilters actionFilters = new ActionFilters(new HashSet<>());

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                CreateIndexRequest createIndexRequest = (CreateIndexRequest) request;
                listener.onResponse((Response) new CreateIndexResponse(false, false, createIndexRequest.index()));
            }
        };

        if (controlled) {

            return new TestTransportBulkAction(
                threadPool,
                transportService,
                clusterService,
                client,
                actionFilters,
                resolver,
                expected::get
            ) {

                @Override
                void executeBulk(
                    Task task,
                    BulkRequest bulkRequest,
                    long startTimeNanos,
                    ActionListener<BulkResponse> listener,
                    String executorName,
                    AtomicArray<BulkItemResponse> responses,
                    Map<String, IndexNotFoundException> indicesThatCannotBeCreated
                ) {
                    expected.set(1000000);
                    super.executeBulk(task, bulkRequest, startTimeNanos, listener, executorName, responses, indicesThatCannotBeCreated);
                }
            };
        } else {
            return new TestTransportBulkAction(
                threadPool,
                transportService,
                clusterService,
                client,
                actionFilters,
                resolver,
                System::nanoTime
            ) {

                @Override
                void executeBulk(
                    Task task,
                    BulkRequest bulkRequest,
                    long startTimeNanos,
                    ActionListener<BulkResponse> listener,
                    String executorName,
                    AtomicArray<BulkItemResponse> responses,
                    Map<String, IndexNotFoundException> indicesThatCannotBeCreated
                ) {
                    long elapsed = spinForAtLeastOneMillisecond();
                    expected.set(elapsed);
                    super.executeBulk(task, bulkRequest, startTimeNanos, listener, executorName, responses, indicesThatCannotBeCreated);
                }
            };
        }
    }

    // test unit conversion with a controlled clock
    public void testTookWithControlledClock() throws Exception {
        runTestTook(true);
    }

    // test took advances with System#nanoTime
    public void testTookWithRealClock() throws Exception {
        runTestTook(false);
    }

    private void runTestTook(boolean controlled) throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk.json");
        // translate Windows line endings (\r\n) to standard ones (\n)
        if (Constants.WINDOWS) {
            bulkAction = Strings.replace(bulkAction, "\r\n", "\n");
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        AtomicLong expected = new AtomicLong();
        TransportBulkAction action = createAction(controlled, expected);
        action.doExecute(null, bulkRequest, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                if (controlled) {
                    assertThat(
                        bulkItemResponses.getTook().getMillis(),
                        equalTo(TimeUnit.MILLISECONDS.convert(expected.get(), TimeUnit.NANOSECONDS))
                    );
                } else {
                    assertThat(
                        bulkItemResponses.getTook().getMillis(),
                        greaterThanOrEqualTo(TimeUnit.MILLISECONDS.convert(expected.get(), TimeUnit.NANOSECONDS))
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }

    static class Resolver extends IndexNameExpressionResolver {
        Resolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }

    static class TestTransportBulkAction extends TransportBulkAction {

        TestTransportBulkAction(
            ThreadPool threadPool,
            TransportService transportService,
            ClusterService clusterService,
            NodeClient client,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            LongSupplier relativeTimeProvider
        ) {
            super(
                threadPool,
                transportService,
                clusterService,
                null,
                client,
                actionFilters,
                indexNameExpressionResolver,
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                relativeTimeProvider
            );
        }
    }
}
