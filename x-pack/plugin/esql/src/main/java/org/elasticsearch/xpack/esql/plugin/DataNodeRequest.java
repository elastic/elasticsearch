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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;
import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDICES_OR_ALIASES_ARRAY;

final class DataNodeRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {
    private static final Logger logger = LogManager.getLogger(DataNodeRequest.class);

    private final String sessionId;
    private final Configuration configuration;
    private final String clusterAlias;
    private final Map<Index, AliasFilter> aliasFilters;
    private final PhysicalPlan plan;
    private List<ShardId> shardIds;
    private String[] indices;
    private final IndicesOptions indicesOptions;
    private final boolean runNodeLevelReduction;

    DataNodeRequest(
        String sessionId,
        Configuration configuration,
        String clusterAlias,
        List<ShardId> shardIds,
        Map<Index, AliasFilter> aliasFilters,
        PhysicalPlan plan,
        String[] indices,
        IndicesOptions indicesOptions,
        boolean runNodeLevelReduction
    ) {
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.clusterAlias = clusterAlias;
        this.shardIds = shardIds;
        this.aliasFilters = aliasFilters;
        this.plan = plan;
        this.indices = indices;
        this.indicesOptions = indicesOptions;
        this.runNodeLevelReduction = runNodeLevelReduction;
    }

    DataNodeRequest(StreamInput in) throws IOException {
        this(in, null);
    }

    /**
     * @param idMapper should always be null in production! Custom mappers are only used in tests to force ID values to be the same after
     *                 serialization and deserialization, which is not the case when they are generated as usual.
     */
    DataNodeRequest(StreamInput in, PlanStreamInput.NameIdMapper idMapper) throws IOException {
        super(in);
        this.sessionId = in.readString();
        this.configuration = new Configuration(
            // TODO make EsqlConfiguration Releasable
            new BlockStreamInput(in, new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE))
        );
        this.clusterAlias = in.readString();
        this.shardIds = in.readCollectionAsList(ShardId::new);
        this.aliasFilters = in.readMap(Index::new, AliasFilter::readFrom);
        PlanStreamInput pin = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration, idMapper);
        this.plan = pin.readNamedWriteable(PhysicalPlan.class);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.runNodeLevelReduction = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sessionId);
        configuration.writeTo(out);
        out.writeString(clusterAlias);
        out.writeCollection(shardIds);
        out.writeMap(aliasFilters);
        new PlanStreamOutput(out, configuration).writeNamedWriteable(plan);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeBoolean(runNodeLevelReduction);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        if (Arrays.equals(NO_INDICES_OR_ALIASES_ARRAY, indices) || Arrays.asList(indices).contains(NO_INDEX_PLACEHOLDER)) {
            logger.trace(() -> format("Indices empty after index resolution, also clearing shardIds %s", shardIds));
            this.shardIds = Collections.emptyList();
        }
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
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

    Configuration configuration() {
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

    /**
     * Returns a map from index UUID to alias filters
     */
    Map<Index, AliasFilter> aliasFilters() {
        return aliasFilters;
    }

    PhysicalPlan plan() {
        return plan;
    }

    boolean runNodeLevelReduction() {
        return runNodeLevelReduction;
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
            && getParentTask().equals(request.getParentTask())
            && Arrays.equals(indices, request.indices)
            && indicesOptions.equals(request.indicesOptions)
            && runNodeLevelReduction == request.runNodeLevelReduction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            sessionId,
            configuration,
            clusterAlias,
            shardIds,
            aliasFilters,
            plan,
            Arrays.hashCode(indices),
            indicesOptions,
            runNodeLevelReduction
        );
    }
}
