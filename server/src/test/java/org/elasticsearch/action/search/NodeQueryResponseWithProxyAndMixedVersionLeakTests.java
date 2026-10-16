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
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportActionProxyTests.SimpleTestRequest;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.search.SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/// Reproduces the Netty buffer leak from incident-management#3360: a batched query phase response
/// ([SearchQueryThenFetchAsyncAction.NodeQueryResponse]) that is forwarded by [TransportActionProxy] between nodes on different
/// transport versions keeps a slice of the inbound network buffer alive after the proxy is done with it.
public class NodeQueryResponseWithProxyAndMixedVersionLeakTests extends ESTestCase {

    private ThreadPool threadPool;
    private MockTransportService coordinatorService;
    private MockTransportService proxyService;
    private MockTransportService targetService;
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void initServices() {
        threadPool = new TestThreadPool(getTestName());
        namedWriteableRegistry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        // The proxy and the coordinator are already on the new build while the target is still on the previous one.
        coordinatorService = buildService(TransportVersion.current());
        proxyService = buildService(TransportVersion.current());
        targetService = buildService(TransportVersionUtils.getPreviousVersion(TransportVersion.current()));
    }

    @After
    public void closeServices() throws Exception {
        IOUtils.close(coordinatorService, proxyService, targetService, () -> terminate(threadPool));
    }

    private MockTransportService buildService(TransportVersion transportVersion) {
        final var service = MockTransportService.createNewService(Settings.EMPTY, VersionInformation.CURRENT, transportVersion, threadPool);
        service.start();
        service.acceptIncomingRequests();
        return service;
    }

    public void testProxiedNodeQueryResponseReleasesInboundBuffer() throws Exception {
        final DiscoveryNode proxyNode = proxyService.getLocalNode();
        final DiscoveryNode targetNode = targetService.getLocalNode();

        // The target data node answers with a batched query phase response carrying partially reduced aggs, serialized
        // the same way as QueryPerNodeState does it (straight into a network buffer sent as a BytesTransportResponse).
        targetService.registerRequestHandler(
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
                try (RecyclerBytesStreamOutput out = targetService.newNetworkBytesStream(null)) {
                    out.setTransportVersion(channel.getVersion());
                    out.writeVInt(0); // no per-shard results, only the merge result really matters here
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

        // Record every buffer slice the response retains while it is read.
        proxyService.registerRequestHandler(
            NODE_SEARCH_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            SimpleTestRequest::new,
            (request, channel, task) -> fail("the proxy must forward the request rather than handle it")
        );
        final List<ReleasableBytesReference> retainedSlices = Collections.synchronizedList(new ArrayList<>());
        TransportActionProxy.registerProxyAction(
            proxyService,
            NODE_SEARCH_ACTION_NAME,
            true,
            in -> new SearchQueryThenFetchAsyncAction.NodeQueryResponse(new SliceRecordingStreamInput(in, retainedSlices)),
            namedWriteableRegistry
        );

        AbstractSimpleTransportTestCase.connectToNode(coordinatorService, proxyNode);
        AbstractSimpleTransportTestCase.connectToNode(proxyService, targetNode);

        final var latch = new CountDownLatch(1);
        final var failure = new AtomicReference<Exception>();
        coordinatorService.sendRequest(
            proxyNode,
            TransportActionProxy.getProxyAction(NODE_SEARCH_ACTION_NAME),
            TransportActionProxy.wrapRequest(targetNode, new SimpleTestRequest("coordinator", true)),
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
        assertThat(failure.get(), nullValue());

        // The versions differ, so the proxy must have deserialized the response and retained at least the aggs slice.
        assertThat(retainedSlices, not(empty()));
        try {
            assertBusy(() -> {
                for (ReleasableBytesReference slice : retainedSlices) {
                    assertFalse(
                        "proxy still holds a slice of the inbound network buffer after forwarding the NodeQueryResponse",
                        slice.hasReferences()
                    );
                }
            });
        } finally {
            // Release what leaked
            for (ReleasableBytesReference slice : retainedSlices) {
                if (slice.hasReferences()) {
                    slice.decRef();
                }
            }
        }
    }

    /// Records the buffer slices that the wrapped reader retains, i.e. the ones it must eventually release.
    private static final class SliceRecordingStreamInput extends FilterStreamInput {
        private final List<ReleasableBytesReference> retainedSlices;

        SliceRecordingStreamInput(StreamInput delegate, List<ReleasableBytesReference> retainedSlices) {
            super(delegate);
            this.retainedSlices = retainedSlices;
        }

        @Override
        public ReleasableBytesReference readReleasableBytesReference() throws IOException {
            return record(super.readReleasableBytesReference());
        }

        @Override
        public ReleasableBytesReference readReleasableBytesReference(int length) throws IOException {
            return record(super.readReleasableBytesReference(length));
        }

        private ReleasableBytesReference record(ReleasableBytesReference slice) {
            retainedSlices.add(slice);
            return slice;
        }
    }
}
