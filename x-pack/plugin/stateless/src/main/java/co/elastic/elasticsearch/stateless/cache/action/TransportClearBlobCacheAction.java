/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache.action;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.Stateless.SharedBlobCacheServiceSupplier;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportClearBlobCacheAction extends TransportNodesAction<
    ClearBlobCacheNodesRequest,
    ClearBlobCacheNodesResponse,
    ClearBlobCacheNodeRequest,
    ClearBlobCacheNodeResponse,
    Void> {
    SharedBlobCacheServiceSupplier sharedBlobCacheServiceSupplier;

    @Inject
    public TransportClearBlobCacheAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SharedBlobCacheServiceSupplier sharedBlobCacheServiceSupplier
    ) {
        super(
            Stateless.CLEAR_BLOB_CACHE_ACTION.name(),
            clusterService,
            transportService,
            actionFilters,
            ClearBlobCacheNodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.sharedBlobCacheServiceSupplier = sharedBlobCacheServiceSupplier;
    }

    @Override
    protected ClearBlobCacheNodesResponse newResponse(
        ClearBlobCacheNodesRequest request,
        List<ClearBlobCacheNodeResponse> clearCacheNodeResponse,
        List<FailedNodeException> failures
    ) {
        return new ClearBlobCacheNodesResponse(clusterService.getClusterName(), clearCacheNodeResponse, failures);
    }

    @Override
    protected ClearBlobCacheNodeRequest newNodeRequest(ClearBlobCacheNodesRequest request) {
        return new ClearBlobCacheNodeRequest();
    }

    @Override
    protected ClearBlobCacheNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new ClearBlobCacheNodeResponse(in);
    }

    @Override
    protected ClearBlobCacheNodeResponse nodeOperation(ClearBlobCacheNodeRequest request, Task task) {
        var sharedBlobCacheService = sharedBlobCacheServiceSupplier.get();
        var cacheEntriesCleared = sharedBlobCacheService.forceEvict(entry -> true);
        return new ClearBlobCacheNodeResponse(clusterService.localNode(), System.currentTimeMillis(), cacheEntriesCleared);
    }
}
