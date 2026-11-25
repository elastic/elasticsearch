/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.BytesTransportResponse;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportActionProxyTests.SimpleTestRequest;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME;

public class ProxiedNodeQueryResponseReleaseTests extends ESTestCase {

    private ThreadPool threadPool;
    private NamedWriteableRegistry namedWriteableRegistry;
    private final List<MockTransportService> services = new ArrayList<>();

    @Before
    public void initThreadPool() {
        threadPool = new TestThreadPool(getTestName());
        namedWriteableRegistry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    @After
    public void closeServices() throws Exception {
        IOUtils.close(services);
        terminate(threadPool);
    }

    public void testMixedVersionProxyReleasesInboundBuffer() throws Exception {
        final ProxiedRead read = proxyNodeQueryResponse(TransportVersionUtils.getPreviousVersion(TransportVersion.current()));
        try {
            assertFalse("the proxy must deserialize the response rather than forward its bytes", read.forwardedRawBytes.get());
            assertAllReleased(read.retained);
        } finally {
            releaseLeaked(read.retained);
        }
    }

    public void testSameVersionProxyReleasesInboundBuffer() throws Exception {
        final ProxiedRead read = proxyNodeQueryResponse(TransportVersion.current());
        try {
            assertTrue("the proxy must forward the bytes of the response", read.forwardedRawBytes.get());
            assertAllReleased(read.retained);
        } finally {
            releaseLeaked(read.retained);
        }
    }

    private static void assertAllReleased(List<ReleasableBytesReference> retained) throws Exception {
        assertBusy(() -> {
            for (ReleasableBytesReference slice : retained) {
                assertFalse("proxy still holds a slice of the inbound network buffer after forwarding the response", slice.hasReferences());
            }
        });
    }

    private static void releaseLeaked(List<ReleasableBytesReference> retained) {
        for (ReleasableBytesReference slice : retained) {
            if (slice.hasReferences()) {
                slice.decRef();
            }
        }
    }

    /**
     * @param retained every slice of the proxy's inbound buffer that was retained while the proxy read the response
     * @param forwardedRawBytes whether the proxy forwarded the received bytes instead of deserializing the response
     */
    private record ProxiedRead(List<ReleasableBytesReference> retained, AtomicBoolean forwardedRawBytes) {}

    /** Sends a batched query phase response through a proxy node. */
    private ProxiedRead proxyNodeQueryResponse(TransportVersion dataNodeVersion) throws Exception {
        final List<ReleasableBytesReference> retained = Collections.synchronizedList(new ArrayList<>());
        final var forwardedRawBytes = new AtomicBoolean();
        final var coordinatorService = buildService(TransportVersion.current(), new TransportInterceptor() {});
        final var proxyService = buildService(TransportVersion.current(), new SliceRecordingInterceptor(retained, forwardedRawBytes));
        final var dataNodeService = buildService(dataNodeVersion, new TransportInterceptor() {});
        final DiscoveryNode proxyNode = proxyService.getLocalNode();
        final DiscoveryNode dataNode = dataNodeService.getLocalNode();

        dataNodeService.registerRequestHandler(
            NODE_SEARCH_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            SimpleTestRequest::new,
            (request, channel, task) -> {
                final var aggs = InternalAggregations.from(
                    List.of(
                        new Min("min_ts", randomDouble(), DocValueFormat.RAW, Map.of()),
                        new Max("max_ts", randomDouble(), DocValueFormat.RAW, Map.of())
                    )
                );
                final var mergeResult = new QueryPhaseResultConsumer.MergeResult(
                    List.of(),
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    DelayableWriteable.referencing(aggs),
                    randomNonNegativeLong()
                );
                try (RecyclerBytesStreamOutput out = dataNodeService.newNetworkBytesStream(null)) {
                    out.setTransportVersion(channel.getVersion());
                    out.writeVInt(0);
                    out.writeBoolean(false);
                    mergeResult.writeTo(out);
                    new SearchPhaseController.TopDocsStats(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO).writeTo(out);
                    final var response = new BytesTransportResponse(out.moveToBytesReference(), out.getTransportVersion());
                    try {
                        channel.sendResponse(response);
                    } finally {
                        response.decRef();
                    }
                }
            }
        );
        proxyService.registerRequestHandler(
            NODE_SEARCH_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            SimpleTestRequest::new,
            (request, channel, task) -> fail("the proxy must forward the request rather than handle it")
        );
        TransportActionProxy.registerProxyAction(
            proxyService,
            NODE_SEARCH_ACTION_NAME,
            true,
            SearchQueryThenFetchAsyncAction.NodeQueryResponse::new,
            namedWriteableRegistry
        );

        AbstractSimpleTransportTestCase.connectToNode(coordinatorService, proxyNode);
        AbstractSimpleTransportTestCase.connectToNode(proxyService, dataNode);

        final var latch = new CountDownLatch(1);
        coordinatorService.sendRequest(
            proxyNode,
            TransportActionProxy.getProxyAction(NODE_SEARCH_ACTION_NAME),
            TransportActionProxy.wrapRequest(dataNode, new SimpleTestRequest("coordinator", true)),
            new TransportResponseHandler<BytesTransportResponse>() {
                @Override
                public BytesTransportResponse read(StreamInput in) throws IOException {
                    return new BytesTransportResponse(in.readAllToReleasableBytesReference(), in.getTransportVersion());
                }

                @Override
                public Executor executor() {
                    return TransportResponseHandler.TRANSPORT_WORKER;
                }

                @Override
                public void handleResponse(BytesTransportResponse response) {
                    latch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    throw new AssertionError("unexpected transport exception", exp);
                }
            }
        );
        safeAwait(latch);
        return new ProxiedRead(retained, forwardedRawBytes);
    }

    private MockTransportService buildService(TransportVersion transportVersion, TransportInterceptor interceptor) {
        final var service = MockTransportService.createNewService(
            Settings.EMPTY,
            MockTransportService.newMockTransport(Settings.EMPTY, transportVersion, threadPool, namedWriteableRegistry),
            VersionInformation.CURRENT,
            threadPool,
            null,
            Set.of(),
            interceptor
        );
        service.start();
        service.acceptIncomingRequests();
        services.add(service);
        return service;
    }

    /** Records the slices of the inbound buffer that a response handler retains while it reads a response. */
    private static final class SliceRecordingInterceptor implements TransportInterceptor {
        private final List<ReleasableBytesReference> retained;
        private final AtomicBoolean forwardedRawBytes;

        SliceRecordingInterceptor(List<ReleasableBytesReference> retained, AtomicBoolean forwardedRawBytes) {
            this.retained = retained;
            this.forwardedRawBytes = forwardedRawBytes;
        }

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            return new AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    if (action.equals(NODE_SEARCH_ACTION_NAME) == false) {
                        sender.sendRequest(connection, action, request, options, handler);
                    } else {
                        sender.sendRequest(connection, action, request, options, new TransportResponseHandler<T>() {
                            @Override
                            public T read(StreamInput in) throws IOException {
                                return handler.read(new SliceRecordingStreamInput(in, retained, forwardedRawBytes));
                            }

                            @Override
                            public Executor executor() {
                                return handler.executor();
                            }

                            @Override
                            public void handleResponse(T response) {
                                handler.handleResponse(response);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                handler.handleException(exp);
                            }
                        });
                    }
                }
            };
        }
    }

    private static final class SliceRecordingStreamInput extends FilterStreamInput {
        private final List<ReleasableBytesReference> retained;
        private final AtomicBoolean forwardedRawBytes;

        SliceRecordingStreamInput(StreamInput delegate, List<ReleasableBytesReference> retained, AtomicBoolean forwardedRawBytes) {
            super(delegate);
            this.retained = retained;
            this.forwardedRawBytes = forwardedRawBytes;
        }

        @Override
        public ReleasableBytesReference readReleasableBytesReference() throws IOException {
            return record(super.readReleasableBytesReference());
        }

        @Override
        public ReleasableBytesReference readReleasableBytesReference(int length) throws IOException {
            return record(super.readReleasableBytesReference(length));
        }

        @Override
        public ReleasableBytesReference readAllToReleasableBytesReference() throws IOException {
            forwardedRawBytes.set(true);
            return record(super.readAllToReleasableBytesReference());
        }

        private ReleasableBytesReference record(ReleasableBytesReference slice) {
            retained.add(slice);
            return slice;
        }
    }
}
