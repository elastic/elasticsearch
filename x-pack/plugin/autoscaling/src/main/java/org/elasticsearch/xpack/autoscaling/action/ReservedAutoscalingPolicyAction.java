/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCalculateCapacityService;

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
 * This Action is the reserved state save version of RestPutAutoscalingPolicyHandler/RestDeleteAutoscalingPolicyHandler
 * <p>
 * It is used by the ReservedClusterStateService to add/update or remove autoscaling policies. Typical usage
 * for this action is in the context of file based settings.
 */
public class ReservedAutoscalingPolicyAction implements ReservedClusterStateHandler<List<PutAutoscalingPolicyAction.Request>> {
    public static final String NAME = "autoscaling";

    private final AutoscalingCalculateCapacityService.Holder policyValidatorHolder;

    /**
     * Creates a ReservedAutoscalingPolicyAction
     *
     * @param policyValidatorHolder requires AutoscalingCalculateCapacityService.Holder for validation
     */
    public ReservedAutoscalingPolicyAction(final AutoscalingCalculateCapacityService.Holder policyValidatorHolder) {
        this.policyValidatorHolder = policyValidatorHolder;
    }

    @Override
    public String name() {
        return NAME;
    }

    private Collection<PutAutoscalingPolicyAction.Request> prepare(List<PutAutoscalingPolicyAction.Request> policies) {
        for (var policy : policies) {
            validate(policy);
        }

        return policies;
    }

    @Override
    public TransformState transform(List<PutAutoscalingPolicyAction.Request> source, TransformState prevState) throws Exception {
        var requests = prepare(source);
        ClusterState state = prevState.state();

        for (var request : requests) {
            state = TransportPutAutoscalingPolicyAction.putAutoscalingPolicy(state, request, policyValidatorHolder.get());
        }

        Set<String> entities = requests.stream().map(r -> r.name()).collect(Collectors.toSet());

        Set<String> toDelete = new HashSet<>(prevState.keys());
        toDelete.removeAll(entities);

        for (var repositoryToDelete : toDelete) {
            state = TransportDeleteAutoscalingPolicyAction.deleteAutoscalingPolicy(state, repositoryToDelete);
        }

        return new TransformState(state, entities);

    }

    @Override
    public List<PutAutoscalingPolicyAction.Request> fromXContent(XContentParser parser) throws IOException {
        List<PutAutoscalingPolicyAction.Request> result = new ArrayList<>();

        Map<String, ?> source = parser.map();

        for (String name : source.keySet()) {
            @SuppressWarnings("unchecked")
            Map<String, ?> content = (Map<String, ?>) source.get(name);
            try (XContentParser policyParser = mapToXContentParser(XContentParserConfiguration.EMPTY, content)) {
                result.add(
                    PutAutoscalingPolicyAction.Request.parse(
                        policyParser,
                        (roles, deciders) -> new PutAutoscalingPolicyAction.Request(
                            RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                            RESERVED_CLUSTER_STATE_HANDLER_IGNORED_TIMEOUT,
                            name,
                            roles,
                            deciders
                        )
                    )
                );
            }
        }

        return result;
    }
}
