/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class DataNodeRequest extends TransportRequest implements IndicesRequest.RemoteClusterShardRequest {
    private static final PlanNameRegistry planNameRegistry = new PlanNameRegistry();
    private final String sessionId;
    private final EsqlConfiguration configuration;
    private final String clusterAlias;
    private final List<ShardId> shardIds;
    private final Map<Index, AliasFilter> aliasFilters;
    private final PhysicalPlan plan;

    private String[] indices; // lazily computed

    DataNodeRequest(
        String sessionId,
        EsqlConfiguration configuration,
        String clusterAlias,
        List<ShardId> shardIds,
        Map<Index, AliasFilter> aliasFilters,
        PhysicalPlan plan
    ) {
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.clusterAlias = clusterAlias;
        this.shardIds = shardIds;
        this.aliasFilters = aliasFilters;
        this.plan = plan;
    }

    DataNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.sessionId = in.readString();
        this.configuration = new EsqlConfiguration(
            // TODO make EsqlConfiguration Releasable
            new BlockStreamInput(in, new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE))
        );
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            this.clusterAlias = in.readString();
        } else {
            this.clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        }
        this.shardIds = in.readCollectionAsList(ShardId::new);
        this.aliasFilters = in.readMap(Index::new, AliasFilter::readFrom);
        this.plan = new PlanStreamInput(in, planNameRegistry, in.namedWriteableRegistry(), configuration).readPhysicalPlanNode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionId);
        configuration.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeString(clusterAlias);
        }
        out.writeCollection(shardIds);
        out.writeMap(aliasFilters);
        new PlanStreamOutput(out, planNameRegistry, configuration).writePhysicalPlanNode(plan);
    }

    @Override
    public String[] indices() {
        if (indices == null) {
            indices = shardIds.stream().map(ShardId::getIndexName).distinct().toArray(String[]::new);
        }
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
                return DataNodeRequest.this.getDescription();
            }
        };
    }

    String sessionId() {
        return sessionId;
    }

    EsqlConfiguration configuration() {
        return configuration;
    }

    QueryPragmas pragmas() {
        return configuration.pragmas();
    }

    String clusterAlias() {
        return clusterAlias;
    }

    List<ShardId> shardIds() {
        return shardIds;
    }

    @Override
    public Collection<ShardId> shards() {
        return shardIds();
    }

    /**
     * Returns a map from index UUID to alias filters
     */
    Map<Index, AliasFilter> aliasFilters() {
        return aliasFilters;
    }

    PhysicalPlan plan() {
        return plan;
    }

    @Override
    public String getDescription() {
        return "shards=" + shardIds + " plan=" + plan;
    }

    @Override
    public String toString() {
        return "DataNodeRequest{" + getDescription() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataNodeRequest request = (DataNodeRequest) o;
        return sessionId.equals(request.sessionId)
            && configuration.equals(request.configuration)
            && clusterAlias.equals(request.clusterAlias)
            && shardIds.equals(request.shardIds)
            && aliasFilters.equals(request.aliasFilters)
            && plan.equals(request.plan)
            && getParentTask().equals(request.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, configuration, clusterAlias, shardIds, aliasFilters, plan);
    }

}
