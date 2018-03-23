/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequestBuilder;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * A {@link LifecycleAction} which shrinks the index.
 */
public class ShrinkAction implements LifecycleAction {
    public static final String NAME = "shrink";
    public static final ParseField NUMBER_OF_SHARDS_FIELD = new ParseField("number_of_shards");

    private static final Logger logger = ESLoggerFactory.getLogger(ShrinkAction.class);
    private static final String SHRUNK_INDEX_NAME_PREFIX = "shrunk-";
    private static final ConstructingObjectParser<ShrinkAction, CreateIndexRequest> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ShrinkAction((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_SHARDS_FIELD);
    }

    private int numberOfShards;
    private AllocationDeciders allocationDeciders;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new CreateIndexRequest());
    }

    public ShrinkAction(int numberOfShards) {
        if (numberOfShards <= 0) {
            throw new IllegalArgumentException("[" + NUMBER_OF_SHARDS_FIELD.getPreferredName() + "] must be greater than 0");
        }
        this.numberOfShards = numberOfShards;
        FilterAllocationDecider decider = new FilterAllocationDecider(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        this.allocationDeciders = new AllocationDeciders(Settings.EMPTY, Collections.singletonList(decider));
    }

    public ShrinkAction(StreamInput in) throws IOException {
        this.numberOfShards = in.readVInt();
    }

    int getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfShards);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUMBER_OF_SHARDS_FIELD.getPreferredName(), numberOfShards);
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(String phase, Index index, Client client, ThreadPool threadPool, LongSupplier nowSupplier) {
        String shrunkenIndexName = SHRUNK_INDEX_NAME_PREFIX + index.getName();
        // TODO(talevy): magical node.name to allocate to
        String nodeName = "MAGIC";
        ClusterStateUpdateStep updateAllocationToOneNode = new ClusterStateUpdateStep(
            "move_to_single_node", NAME, phase, index.getName(), (clusterState) -> {
            IndexMetaData idxMeta = clusterState.metaData().index(index);
            if (idxMeta == null) {
                return clusterState;
            }
            Settings.Builder newSettings = Settings.builder()
                .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_PREFIX, "")
                .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX, "")
                .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeName);
            return ClusterState.builder(clusterState)
                .metaData(MetaData.builder(clusterState.metaData())
                    .updateSettings(newSettings.build(), index.getName())).build();
        });

//            resizeRequest.getTargetIndexRequest().settings(Settings.builder()
//                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
//                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, indexMetaData.getNumberOfReplicas())
//                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, indexMetaData.getCreationDate())
//                .build());
//            indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
//                resizeRequest.getTargetIndexRequest().alias(new Alias(aliasMetaDataObjectCursor.value.alias()));
//            });

        // TODO(talevy): needs access to original index metadata, not just Index
        int numReplicas = -1;
        long lifecycleDate = -1L;
        Settings targetIndexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, lifecycleDate)
            .build();
        CreateIndexRequest targetIndexRequest = new CreateIndexRequest(shrunkenIndexName, targetIndexSettings);
        // TODO(talevy): need access to indexmetadata
//            indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
//                resizeRequest.getTargetIndexRequest().alias(new Alias(aliasMetaDataObjectCursor.value.alias()));
//            });

        ClientStep<ResizeRequestBuilder, ResizeResponse> shrinkStep = new ClientStep<>( "segment_count",
            NAME, phase, index.getName(),

            client.admin().indices().prepareResizeIndex(index.getName(), shrunkenIndexName).setTargetIndex(targetIndexRequest),
            currentState -> {
                // check that shrunken index was already created, if so, no need to both client
                IndexMetaData shrunkMetaData = currentState.metaData().index(shrunkenIndexName);
                boolean isSuccessful = shrunkMetaData != null && shrunkenIndexName.equals(IndexMetaData.INDEX_SHRINK_SOURCE_NAME
                    .get(shrunkMetaData.getSettings()));

            }, ResizeResponse::isAcknowledged);


        ConditionalWaitStep shrunkenIndexIsAllocated = new ConditionalWaitStep("wait_replicas_allocated", NAME,
            phase, index.getName(), (currentState) -> ActiveShardCount.ALL.enoughShardsActive(currentState, index.getName()) );

        ClusterStateUpdateStep deleteAndUpdateAliases = new ClusterStateUpdateStep(
            "delete_this_index_set_aliases_on_shrunken", NAME, phase, index.getName(), (clusterState) -> {
            IndexMetaData idxMeta = clusterState.metaData().index(index);
            if (idxMeta == null) {
                return clusterState;
            }

            // TODO(talevy): expose - MetadataDeleteIndexService.deleteIndices(clusterState, Set.of(index.getName()))
            // also, looks like deletes are special CS tasks
            // AckedClusterStateUpdateTask, Priority.URGENT

            // 1. delete index
            // 2. assign alias to shrunken index
            // 3. assign index.lifecycle settings to shrunken index
            return clusterState;
        });

        return Arrays.asList(updateAllocationToOneNode,
            AllocateAction.getAllocationCheck(allocationDeciders, phase, index.getName()),
            shrinkStep, shrunkenIndexIsAllocated, deleteAndUpdateAliases);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShrinkAction that = (ShrinkAction) o;
        return Objects.equals(numberOfShards, that.numberOfShards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfShards);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
