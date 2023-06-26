/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleService;

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
 * This {@link ReservedClusterStateHandler} is responsible for reserved state
 * CRUD operations on SLM policies in, e.g. file based settings.
 * <p>
 * Internally it uses {@link TransportPutSnapshotLifecycleAction} and
 * {@link TransportDeleteSnapshotLifecycleAction} to add, update and delete ILM policies.
 */
public class ReservedSnapshotAction implements ReservedClusterStateHandler<List<SnapshotLifecyclePolicy>> {

    public static final String NAME = "slm";

    public ReservedSnapshotAction() {}

    @Override
    public String name() {
        return NAME;
    }

    private Collection<PutSnapshotLifecycleAction.Request> prepare(List<SnapshotLifecyclePolicy> policies, ClusterState state) {
        List<PutSnapshotLifecycleAction.Request> result = new ArrayList<>();

        List<Exception> exceptions = new ArrayList<>();

        for (var policy : policies) {
            PutSnapshotLifecycleAction.Request request = new PutSnapshotLifecycleAction.Request(policy.getId(), policy);
            try {
                validate(request);
                SnapshotLifecycleService.validateRepositoryExists(request.getLifecycle().getRepository(), state);
                SnapshotLifecycleService.validateMinimumInterval(request.getLifecycle(), state);
                result.add(request);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        if (exceptions.isEmpty() == false) {
            var illegalArgumentException = new IllegalArgumentException("Error on validating SLM requests");
            exceptions.forEach(illegalArgumentException::addSuppressed);
            throw illegalArgumentException;
        }

        return result;
    }

    @Override
    public TransformState transform(Object source, TransformState prevState) throws Exception {
        @SuppressWarnings("unchecked")
        var requests = prepare((List<SnapshotLifecyclePolicy>) source, prevState.state());

        ClusterState state = prevState.state();

        for (var request : requests) {
            TransportPutSnapshotLifecycleAction.UpdateSnapshotPolicyTask task =
                new TransportPutSnapshotLifecycleAction.UpdateSnapshotPolicyTask(request);

            state = task.execute(state);
        }

        Set<String> entities = requests.stream().map(r -> r.getLifecycle().getId()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        for (var policyToDelete : toDelete) {
            var task = new TransportDeleteSnapshotLifecycleAction.DeleteSnapshotPolicyTask(policyToDelete);
            state = task.execute(state);
        }

        return new TransformState(state, entities);
    }

    @Override
    public List<SnapshotLifecyclePolicy> fromXContent(XContentParser parser) throws IOException {
        List<SnapshotLifecyclePolicy> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (String name : source.keySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(name);
            try (XContentParser policyParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                result.add(SnapshotLifecyclePolicy.parse(policyParser, name));
            }
        }

        return result;
    }

    @Override
    public Collection<String> optionalDependencies() {
        return List.of("snapshot_repositories");
    }
}
