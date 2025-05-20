/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.repositories.reservedstate;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

/**
 * This Action is the reserved state save version of RestPutRepositoryAction/RestDeleteRepositoryAction
 * <p>
 * It is used by the ReservedClusterStateService to add/update or remove snapshot repositories. Typical usage
 * for this action is in the context of file based settings.
 */
public class ReservedRepositoryAction implements ReservedClusterStateHandler<ClusterState, List<PutRepositoryRequest>> {
    public static final String NAME = "snapshot_repositories";

    private final RepositoriesService repositoriesService;

    /**
     * Creates a ReservedRepositoryAction
     *
     * @param repositoriesService requires RepositoriesService for the cluster state operations
     */
    public ReservedRepositoryAction(RepositoriesService repositoriesService) {
        this.repositoriesService = repositoriesService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    public Collection<PutRepositoryRequest> prepare(Object input) {
        List<PutRepositoryRequest> repositories = (List<PutRepositoryRequest>) input;

        for (var repositoryRequest : repositories) {
            validate(repositoryRequest);
            RepositoriesService.validateRepositoryName(repositoryRequest.name());
            repositoriesService.validateRepositoryCanBeCreated(repositoryRequest);
        }

        return repositories;
    }

    @Override
    public TransformState<ClusterState> transform(List<PutRepositoryRequest> source, TransformState<ClusterState> prevState)
        throws Exception {
        var requests = prepare(source);

        ClusterState state = prevState.state();

        for (var request : requests) {
            RepositoriesService.RegisterRepositoryTask task = new RepositoriesService.RegisterRepositoryTask(repositoriesService, request);
            state = task.execute(state);
        }

        Set<String> entities = requests.stream().map(r -> r.name()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        for (var repositoryToDelete : toDelete) {
            var task = new RepositoriesService.UnregisterRepositoryTask(RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT, repositoryToDelete);
            state = task.execute(state);
        }

        return new TransformState<>(state, entities);

    }

    @Override
    public List<PutRepositoryRequest> fromXContent(XContentParser parser) throws IOException {
        List<PutRepositoryRequest> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (var entry : source.entrySet()) {
            PutRepositoryRequest putRepositoryRequest = new PutRepositoryRequest(
                RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                entry.getKey()
            );
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) entry.getValue();
            try (XContentParser repoParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                putRepositoryRequest.source(repoParser.mapOrdered());
            }
            result.add(putRepositoryRequest);
        }

        return result;
    }
}
