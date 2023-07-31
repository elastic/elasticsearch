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

package co.elastic.elasticsearch.stateless.metering.action;

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportGetBlobStoreStatsAction extends TransportNodesAction<
    GetBlobStoreStatsNodesRequest,
    GetBlobStoreStatsNodesResponse,
    GetBlobStoreStatsNodeRequest,
    GetBlobStoreStatsNodeResponse> {
    private final ObjectStoreService objectStoreService;

    @Inject
    public TransportGetBlobStoreStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ObjectStoreService objectStoreService
    ) {
        super(
            GetBlobStoreStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            GetBlobStoreStatsNodesRequest::new,
            GetBlobStoreStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.objectStoreService = objectStoreService;
    }

    @Override
    protected GetBlobStoreStatsNodesResponse newResponse(
        GetBlobStoreStatsNodesRequest request,
        List<GetBlobStoreStatsNodeResponse> getBlobStoreStatsNodeResponses,
        List<FailedNodeException> failures
    ) {
        return new GetBlobStoreStatsNodesResponse(clusterService.getClusterName(), getBlobStoreStatsNodeResponses, failures);
    }

    @Override
    protected GetBlobStoreStatsNodeRequest newNodeRequest(GetBlobStoreStatsNodesRequest request) {
        return new GetBlobStoreStatsNodeRequest();
    }

    @Override
    protected GetBlobStoreStatsNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetBlobStoreStatsNodeResponse(in);
    }

    @Override
    protected GetBlobStoreStatsNodeResponse nodeOperation(GetBlobStoreStatsNodeRequest request, Task task) {
        final BlobStoreRepository blobStoreRepository = objectStoreService.getObjectStore();
        return new GetBlobStoreStatsNodeResponse(clusterService.localNode(), blobStoreRepository.stats());
    }
}
