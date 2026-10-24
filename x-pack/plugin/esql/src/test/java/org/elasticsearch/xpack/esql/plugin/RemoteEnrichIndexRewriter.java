/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lets a test play the part of a querying cluster that tampers with the plan it sends to a remote cluster, by pointing the
 * remote ENRICH at an arbitrary index instead of the concrete index of the enrich policy.
 */
public final class RemoteEnrichIndexRewriter {

    private RemoteEnrichIndexRewriter() {}

    /**
     * Rewrites every {@link ClusterComputeRequest} that {@code transportServices} send to {@code clusterAlias} so that its ENRICH
     * reads {@code index} on that cluster. Returns the number of requests whose plan contained an ENRICH and was therefore changed.
     * Call {@link MockTransportService#clearAllRules()} to stop rewriting.
     */
    public static AtomicInteger rewriteRemoteEnrichIndex(Iterable<TransportService> transportServices, String clusterAlias, String index) {
        AtomicInteger rewritten = new AtomicInteger();
        for (TransportService ts : transportServices) {
            ((MockTransportService) ts).addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(ComputeService.CLUSTER_ACTION_NAME)
                    && request instanceof ClusterComputeRequest original
                    && original.clusterAlias().equals(clusterAlias)) {
                    RemoteClusterPlan plan = original.remoteClusterPlan();
                    PhysicalPlan tamperedPlan = withEnrichIndex(plan.plan(), clusterAlias, index);
                    if (tamperedPlan.equals(plan.plan()) == false) {
                        rewritten.incrementAndGet();
                    }
                    ClusterComputeRequest tampered = new ClusterComputeRequest(
                        original.clusterAlias(),
                        original.sessionId(),
                        original.configuration(),
                        new RemoteClusterPlan(tamperedPlan, plan.targetIndices(), plan.originalIndices())
                    );
                    tampered.setParentTask(original.getParentTask());
                    connection.sendRequest(requestId, action, tampered, options);
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            });
        }
        return rewritten;
    }

    private static PhysicalPlan withEnrichIndex(PhysicalPlan plan, String clusterAlias, String index) {
        PhysicalPlan rewritten = plan.transformUp(
            EnrichExec.class,
            e -> new EnrichExec(
                e.source(),
                e.child(),
                e.mode(),
                e.matchType(),
                e.matchField(),
                e.policyName(),
                e.policyMatchField(),
                withIndex(e.concreteIndices(), clusterAlias, index),
                e.enrichFields()
            )
        );
        return rewritten.transformUp(
            FragmentExec.class,
            f -> f.withFragment(
                f.fragment()
                    .transformUp(
                        Enrich.class,
                        e -> new Enrich(
                            e.source(),
                            e.child(),
                            e.mode(),
                            e.policyName(),
                            e.matchField(),
                            e.policy(),
                            withIndex(e.concreteIndices(), clusterAlias, index),
                            e.enrichFields()
                        )
                    )
            )
        );
    }

    private static Map<String, String> withIndex(Map<String, String> concreteIndices, String clusterAlias, String index) {
        Map<String, String> result = new HashMap<>(concreteIndices);
        result.put(clusterAlias, index);
        return result;
    }
}
