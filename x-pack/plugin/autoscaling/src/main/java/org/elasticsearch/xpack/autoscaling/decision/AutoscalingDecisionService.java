/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.autoscaling.Autoscaling;
import org.elasticsearch.xpack.autoscaling.AutoscalingMetadata;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AutoscalingDecisionService {
    private Map<String, AutoscalingDeciderService<? extends AutoscalingDeciderConfiguration>> deciderByName;

    public AutoscalingDecisionService(Set<AutoscalingDeciderService<? extends AutoscalingDeciderConfiguration>> deciders) {
        assert deciders.size() >= 1; // always have always
        this.deciderByName = deciders.stream().collect(Collectors.toMap(AutoscalingDeciderService::name, Function.identity()));
    }

    public static class Holder {
        private final Autoscaling autoscaling;
        private final SetOnce<AutoscalingDecisionService> servicesSetOnce = new SetOnce<>();

        public Holder(Autoscaling autoscaling) {
            this.autoscaling = autoscaling;
        }

        public AutoscalingDecisionService get() {
            // defer constructing services until transport action creation time.
            AutoscalingDecisionService autoscalingDecisionService = servicesSetOnce.get();
            if (autoscalingDecisionService == null) {
                autoscalingDecisionService = new AutoscalingDecisionService(autoscaling.createDeciderServices());
                servicesSetOnce.set(autoscalingDecisionService);
            }

            return autoscalingDecisionService;
        }
    }

    public SortedMap<String, AutoscalingDecisions> decide(ClusterState state) {
        AutoscalingDeciderContext context = () -> state;

        AutoscalingMetadata autoscalingMetadata = state.metadata().custom(AutoscalingMetadata.NAME);
        if (autoscalingMetadata != null) {
            return new TreeMap<>(
                autoscalingMetadata.policies()
                    .entrySet()
                    .stream()
                    .map(e -> Tuple.tuple(e.getKey(), getDecision(e.getValue().policy(), context)))
                    .collect(Collectors.toMap(Tuple::v1, Tuple::v2))
            );
        } else {
            return new TreeMap<>();
        }
    }

    private AutoscalingDecisions getDecision(AutoscalingPolicy policy, AutoscalingDeciderContext context) {
        Collection<AutoscalingDecision> decisions = policy.deciders()
            .values()
            .stream()
            .map(decider -> getDecision(decider, context))
            .collect(Collectors.toList());
        return new AutoscalingDecisions(decisions);
    }

    private <T extends AutoscalingDeciderConfiguration> AutoscalingDecision getDecision(T decider, AutoscalingDeciderContext context) {
        assert deciderByName.containsKey(decider.name());
        @SuppressWarnings("unchecked")
        AutoscalingDeciderService<T> service = (AutoscalingDeciderService<T>) deciderByName.get(decider.name());
        return service.scale(decider, context);
    }
}
