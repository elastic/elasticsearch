/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.reservedstate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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

import static org.elasticsearch.client.internal.Requests.putRepositoryRequest;
import static org.elasticsearch.common.xcontent.XContentHelper.mapToXContentParser;

public class ReservedRepositoryAction implements ReservedClusterStateHandler<List<PutRepositoryRequest>> {
    public static final String NAME = "snapshot_repositories";

    private final RepositoriesService repositoriesService;

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
            repositoriesService.validateRepository(repositoryRequest, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {}

                @Override
                public void onFailure(Exception e) {
                    throw new IllegalStateException("Validation error", e);
                }
            });
        }

        return repositories;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
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
            var task = new RepositoriesService.UnregisterRepositoryTask(repositoryToDelete);
            state = task.execute(state);
        }

        return new TransformState(state, entities);

    }

    @Override
    public List<PutRepositoryRequest> fromXContent(XContentParser parser) throws IOException {
        List<PutRepositoryRequest> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (String name : source.keySet()) {
            PutRepositoryRequest putRepositoryRequest = putRepositoryRequest(name);
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(name);
            try (XContentParser repoParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                putRepositoryRequest.source(repoParser.mapOrdered());
            }
            result.add(putRepositoryRequest);
        }

        return result;
    }
}
