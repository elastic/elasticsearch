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

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.StatelessComponents;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryStatsSnapshot;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportGetBlobStoreStatsAction extends TransportNodesAction<
    GetBlobStoreStatsNodesRequest,
    GetBlobStoreStatsNodesResponse,
    GetBlobStoreStatsNodeRequest,
    GetBlobStoreStatsNodeResponse,
    Void> {
    private final ObjectStoreService objectStoreService;
    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetBlobStoreStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        StatelessComponents statelessComponents,
        RepositoriesService repositoriesService
    ) {
        super(
            Stateless.GET_BLOB_STORE_STATS_ACTION.name(),
            clusterService,
            transportService,
            actionFilters,
            GetBlobStoreStatsNodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.objectStoreService = statelessComponents.getObjectStoreService();
        this.repositoriesService = repositoriesService;
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
        return new GetBlobStoreStatsNodeResponse(
            clusterService.localNode(),
            objectStoreService.stats(),
            computeObsRepositoryStats(repositoriesService)
        );
    }

    // Package private for testing
    /**
     * This method aggregates repositoryStats from all snapshot repositories. Since repositories of different
     * types, e.g. s3, azure, have different metric names, the assumption here is that all snapshot repositories
     * have the same type so that their metrics can be meaningfully aggregated.
     * In practice, this should always be true since a cluster runs on a single CSP region. In addition, it is
     * most likely there will be only a single snapshot repository per cluster.
     */
    static RepositoryStats computeObsRepositoryStats(RepositoriesService repositoriesService) {
        final List<RepositoryStatsSnapshot> repositoryStatsSnapshots = repositoriesService.repositoriesStats();
        if (Assertions.ENABLED) {
            final Set<String> repositoryTypes = repositoryStatsSnapshots.stream()
                .map(repositoryStatsSnapshot -> repositoryStatsSnapshot.getRepositoryInfo().type)
                .collect(Collectors.toUnmodifiableSet());
            assert repositoryTypes.size() <= 1 : "expect at most a single repository type, but got " + repositoryTypes;
        }
        return repositoryStatsSnapshots.stream()
            .map(RepositoryStatsSnapshot::getRepositoryStats)
            .reduce(RepositoryStats::merge)
            .orElse(RepositoryStats.EMPTY_STATS);
    }
}
