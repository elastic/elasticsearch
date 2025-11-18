/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TransportNodesHotThreadsAction extends TransportNodesAction<
    NodesHotThreadsRequest,
    NodesHotThreadsResponse,
    TransportNodesHotThreadsAction.NodeRequest,
    NodeHotThreads,
    Void> {

    public static final ActionType<NodesHotThreadsResponse> TYPE = new ActionType<>("cluster:monitor/nodes/hot_threads");

    @Inject
    public TransportNodesHotThreadsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(
            TYPE.name(),
            clusterService,
            transportService,
            actionFilters,
            NodeRequest::new,
            threadPool.executor(ThreadPool.Names.GENERIC)
        );
    }

    @Override
    protected NodesHotThreadsResponse newResponse(
        NodesHotThreadsRequest request,
        List<NodeHotThreads> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesHotThreadsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(NodesHotThreadsRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodeHotThreads newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeHotThreads(in);
    }

    @Override
    protected NodeHotThreads nodeOperation(NodeRequest request, Task task) {
        final var hotThreads = new HotThreads().busiestThreads(request.requestOptions.threads())
            .type(request.requestOptions.reportType())
            .sortOrder(request.requestOptions.sortOrder())
            .interval(request.requestOptions.interval())
            .threadElementsSnapshotCount(request.requestOptions.snapshots())
            .ignoreIdleThreads(request.requestOptions.ignoreIdleThreads());
        final var out = transportService.newNetworkBytesStream();
        final var trackedResource = LeakTracker.wrap(out);
        var success = false;
        try {
            try (var writer = new OutputStreamWriter(Streams.flushOnCloseStream(out), StandardCharsets.UTF_8)) {
                hotThreads.detect(writer);
            }
            final var result = new NodeHotThreads(clusterService.localNode(), new ReleasableBytesReference(out.bytes(), trackedResource));
            success = true;
            return result;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to detect hot threads", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(trackedResource);
            }
        }
    }

    public static class NodeRequest extends AbstractTransportRequest {

        final HotThreads.RequestOptions requestOptions;

        NodeRequest(NodesHotThreadsRequest request) {
            this.requestOptions = request.requestOptions;
        }

        NodeRequest(StreamInput in) throws IOException {
            super(in);
            requestOptions = HotThreads.RequestOptions.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            requestOptions.writeTo(out);
        }
    }
}
