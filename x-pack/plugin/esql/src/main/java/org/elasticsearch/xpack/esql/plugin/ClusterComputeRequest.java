/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * A request to initiate a compute on a remote cluster. The output pages of the compute on the remote cluster will be placed in an
 * exchange sink specified by the {@code sessionId}. The exchange sink associated with this {@code sessionId} should have been opened
 * via {@link ExchangeService#openExchange} before sending this request to the remote cluster. The coordinator on the main cluster
 * will poll pages from this sink. Internally, this compute will trigger sub-computes on data nodes via {@link DataNodeRequest}.
 */
final class ClusterComputeRequest extends TransportRequest implements IndicesRequest.Replaceable {
    private static final PlanNameRegistry planNameRegistry = new PlanNameRegistry();
    private final String clusterAlias;
    private final String sessionId;
    private final EsqlConfiguration configuration;
    private final PhysicalPlan plan;

    private String[] indices;
    private final OriginalIndices originalIndices;
    private final String[] targetIndices;

    /**
     * A request to start a compute on a remote cluster.
     *
     * @param clusterAlias    the cluster alias of this remote cluster
     * @param sessionId       the sessionId in which the output pages will be placed in the exchange sink specified by this id
     * @param configuration   the configuration for this compute
     * @param plan            the physical plan to be executed
     * @param targetIndices   the target indices
     * @param originalIndices the original indices - needed to resolve alias filters
     */
    ClusterComputeRequest(
        String clusterAlias,
        String sessionId,
        EsqlConfiguration configuration,
        PhysicalPlan plan,
        String[] targetIndices,
        OriginalIndices originalIndices
    ) {
        this.clusterAlias = clusterAlias;
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.plan = plan;
        this.targetIndices = targetIndices;
        this.originalIndices = originalIndices;
    }

    ClusterComputeRequest(StreamInput in) throws IOException {
        super(in);
        this.clusterAlias = in.readString();
        this.sessionId = in.readString();
        this.configuration = new EsqlConfiguration(
            // TODO make EsqlConfiguration Releasable
            new BlockStreamInput(in, new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE))
        );
        this.plan = new PlanStreamInput(in, planNameRegistry, in.namedWriteableRegistry(), configuration).readPhysicalPlanNode();
        this.targetIndices = in.readStringArray();
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_ORIGINAL_INDICES)) {
            this.originalIndices = OriginalIndices.readOriginalIndices(in);
        } else {
            this.originalIndices = new OriginalIndices(in.readStringArray(), IndicesOptions.strictSingleIndexNoExpandForbidClosed());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterAlias);
        out.writeString(sessionId);
        configuration.writeTo(out);
        new PlanStreamOutput(out, planNameRegistry, configuration).writePhysicalPlanNode(plan);
        out.writeStringArray(targetIndices);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_ORIGINAL_INDICES)) {
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        } else {
            out.writeStringArray(originalIndices.indices());
        }
    }

    @Override
    public String[] indices() {
        return indices != null ? indices : originalIndices.indices();
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (parentTaskId.isSet() == false) {
            assert false : "DataNodeRequest must have a parent task";
            throw new IllegalStateException("DataNodeRequest must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return ClusterComputeRequest.this.getDescription();
            }
        };
    }

    OriginalIndices originalIndices() {
        return originalIndices;
    }

    String[] targetIndices() {
        return targetIndices;
    }

    String clusterAlias() {
        return clusterAlias;
    }

    String sessionId() {
        return sessionId;
    }

    EsqlConfiguration configuration() {
        return configuration;
    }

    PhysicalPlan plan() {
        return plan;
    }

    @Override
    public String getDescription() {
        return "indices=" + Arrays.toString(targetIndices) + " plan=" + plan;
    }

    @Override
    public String toString() {
        return "ClusterComputeRequest{" + getDescription() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterComputeRequest request = (ClusterComputeRequest) o;
        return clusterAlias.equals(request.clusterAlias)
            && sessionId.equals(request.sessionId)
            && configuration.equals(request.configuration)
            && Arrays.equals(targetIndices, request.targetIndices)
            && originalIndices.equals(request.originalIndices)
            && plan.equals(request.plan)
            && getParentTask().equals(request.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, configuration, Arrays.hashCode(targetIndices), originalIndices, plan);
    }
}
