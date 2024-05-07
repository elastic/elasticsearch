/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.IndicesRequest;
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
final class ClusterComputeRequest extends TransportRequest implements IndicesRequest {
    private static final PlanNameRegistry planNameRegistry = new PlanNameRegistry();
    private final String clusterAlias;
    private final String sessionId;
    private final EsqlConfiguration configuration;
    private final PhysicalPlan plan;

    private final String[] originalIndices;
    private final String[] indices;

    /**
     * A request to start a compute on a remote cluster.
     *
     * @param clusterAlias the cluster alias of this remote cluster
     * @param sessionId the sessionId in which the output pages will be placed in the exchange sink specified by this id
     * @param configuration the configuration for this compute
     * @param plan the physical plan to be executed
     * @param indices the target indices
     * @param originalIndices the original indices - needed to resolve alias filters
     */
    ClusterComputeRequest(
        String clusterAlias,
        String sessionId,
        EsqlConfiguration configuration,
        PhysicalPlan plan,
        String[] indices,
        String[] originalIndices
    ) {
        this.clusterAlias = clusterAlias;
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.plan = plan;
        this.indices = indices;
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
        this.indices = in.readStringArray();
        this.originalIndices = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterAlias);
        out.writeString(sessionId);
        configuration.writeTo(out);
        new PlanStreamOutput(out, planNameRegistry, configuration).writePhysicalPlanNode(plan);
        out.writeStringArray(indices);
        out.writeStringArray(originalIndices);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
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

    String clusterAlias() {
        return clusterAlias;
    }

    String sessionId() {
        return sessionId;
    }

    EsqlConfiguration configuration() {
        return configuration;
    }

    String[] originalIndices() {
        return originalIndices;
    }

    PhysicalPlan plan() {
        return plan;
    }

    @Override
    public String getDescription() {
        return "indices=" + Arrays.toString(indices) + " plan=" + plan;
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
            && Arrays.equals(indices, request.indices)
            && Arrays.equals(originalIndices, request.originalIndices)
            && plan.equals(request.plan)
            && getParentTask().equals(request.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, configuration, Arrays.hashCode(indices), Arrays.hashCode(originalIndices), plan);
    }
}
