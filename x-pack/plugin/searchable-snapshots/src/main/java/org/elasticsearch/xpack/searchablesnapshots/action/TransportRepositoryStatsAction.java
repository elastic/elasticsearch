/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TransportRepositoryStatsAction extends TransportNodesAction<
    RepositoryStatsRequest,
    RepositoryStatsResponse,
    RepositoryStatsNodeRequest,
    RepositoryStatsNodeResponse> {

    private final RepositoriesService repositoriesService;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportRepositoryStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService,
        XPackLicenseState licenseState
    ) {
        super(
            RepositoryStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            RepositoryStatsRequest::new,
            RepositoryStatsNodeRequest::new,
            ThreadPool.Names.SAME,
            RepositoryStatsNodeResponse.class
        );
        this.repositoriesService = repositoriesService;
        this.licenseState = Objects.requireNonNull(licenseState);
    }

    @Override
    protected RepositoryStatsResponse newResponse(
        RepositoryStatsRequest request,
        List<RepositoryStatsNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        return new RepositoryStatsResponse(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected RepositoryStatsNodeRequest newNodeRequest(RepositoryStatsRequest request) {
        return new RepositoryStatsNodeRequest(request.getRepository());
    }

    @Override
    protected RepositoryStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new RepositoryStatsNodeResponse(in);
    }

    @Override
    protected RepositoryStatsNodeResponse nodeOperation(RepositoryStatsNodeRequest request, Task task) {
        SearchableSnapshots.ensureValidLicense(licenseState);
        if (clusterService.localNode().isMasterNode() == false && clusterService.localNode().isDataNode() == false) {
            return new RepositoryStatsNodeResponse(clusterService.localNode(), RepositoryStats.EMPTY_STATS);
        }
        final Repository repository = repositoriesService.repository(request.getRepository());
        return new RepositoryStatsNodeResponse(clusterService.localNode(), repository.stats());
    }
}
