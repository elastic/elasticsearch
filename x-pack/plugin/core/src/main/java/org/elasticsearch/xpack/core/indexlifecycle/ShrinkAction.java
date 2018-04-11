/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which shrinks the index.
 */
public class ShrinkAction implements LifecycleAction {
    public static final String NAME = "shrink";
    public static final ParseField NUMBER_OF_SHARDS_FIELD = new ParseField("number_of_shards");

//    private static final String SHRUNK_INDEX_NAME_PREFIX = "shrunk-";
    private static final ConstructingObjectParser<ShrinkAction, CreateIndexRequest> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ShrinkAction((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_SHARDS_FIELD);
    }

    private int numberOfShards;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new CreateIndexRequest());
    }

    public ShrinkAction(int numberOfShards) {
        if (numberOfShards <= 0) {
            throw new IllegalArgumentException("[" + NUMBER_OF_SHARDS_FIELD.getPreferredName() + "] must be greater than 0");
        }
        this.numberOfShards = numberOfShards;
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
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
//        String shrunkenIndexName = SHRUNK_INDEX_NAME_PREFIX + index.getName();
//        // TODO(talevy): magical node.name to allocate to
//        String nodeName = "MAGIC";
//        ClusterStateUpdateStep updateAllocationToOneNode = new ClusterStateUpdateStep(
//            "move_to_single_node", NAME, phase, index.getName(), (clusterState) -> {
//            IndexMetaData idxMeta = clusterState.metaData().index(index);
//            if (idxMeta == null) {
//                return clusterState;
//            }
//            Settings.Builder newSettings = Settings.builder()
//                .put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_PREFIX, "")
//                .put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX, "")
//                .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", nodeName);
//            return ClusterState.builder(clusterState)
//                .metaData(MetaData.builder(clusterState.metaData())
//                    .updateSettings(newSettings.build(), index.getName())).build();
//        });

//            resizeRequest.getTargetIndexRequest().settings(Settings.builder()
//                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
//                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, indexMetaData.getNumberOfReplicas())
//                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, indexMetaData.getCreationDate())
//                .build());
//            indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
//                resizeRequest.getTargetIndexRequest().alias(new Alias(aliasMetaDataObjectCursor.value.alias()));
//            });

//        // TODO(talevy): needs access to original index metadata, not just Index
//        int numReplicas = -1;
//        long lifecycleDate = -1L;
//        Settings targetIndexSettings = Settings.builder()
//            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
//            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas)
//            .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, lifecycleDate)
//            .build();
//        CreateIndexRequest targetIndexRequest = new CreateIndexRequest(shrunkenIndexName, targetIndexSettings);
        // TODO(talevy): need access to indexmetadata
//            indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
//                resizeRequest.getTargetIndexRequest().alias(new Alias(aliasMetaDataObjectCursor.value.alias()));
//            });

//        ClientStep<ResizeRequestBuilder, ResizeResponse> shrinkStep = new ClientStep<>( "segment_count",
//            NAME, phase, index.getName(),
//
//            client.admin().indices().prepareResizeIndex(index.getName(), shrunkenIndexName).setTargetIndex(targetIndexRequest),
//            currentState -> {
//                // check that shrunken index was already created, if so, no need to both client
//                IndexMetaData shrunkMetaData = currentState.metaData().index(shrunkenIndexName);
//                return shrunkMetaData != null && shrunkenIndexName.equals(IndexMetaData.INDEX_SHRINK_SOURCE_NAME
//                    .get(shrunkMetaData.getSettings()));
//
//            }, ResizeResponse::isAcknowledged);
//
//
//        ConditionalWaitStep shrunkenIndexIsAllocated = new ConditionalWaitStep("wait_replicas_allocated", NAME,
//            phase, index.getName(), (currentState) -> ActiveShardCount.ALL.enoughShardsActive(currentState, index.getName()) );
//
//        ClusterStateUpdateStep deleteAndUpdateAliases = new ClusterStateUpdateStep(
//            "delete_this_index_set_aliases_on_shrunken", NAME, phase, index.getName(), (clusterState) -> {
//            IndexMetaData idxMeta = clusterState.metaData().index(index);
//            if (idxMeta == null) {
//                return clusterState;
//            }

            // TODO(talevy): expose - MetadataDeleteIndexService.deleteIndices(clusterState, Set.of(index.getName()))
            // also, looks like deletes are special CS tasks
            // AckedClusterStateUpdateTask, Priority.URGENT

            // 1. delete index
            // 2. assign alias to shrunken index
            // 3. assign index.lifecycle settings to shrunken index
//            return clusterState;
//        });

//        UpdateSettingsStep allocateStep = new UpdateSettingsStep();
//        UpdateSettingsStep waitForAllocation = new UpdateSettingsStep();
//        UpdateSettingsStep allocateStep = new UpdateSettingsStep();
//        Step.StepKey allocateKey = new StepKey(phase, NAME, NAME);
//        StepKey allocationRoutedKey = new StepKey(phase, NAME, AllocationRoutedStep.NAME);
//
//        Settings.Builder newSettings = Settings.builder();
//        newSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", value));
//        UpdateSettingsStep allocateStep = new UpdateSettingsStep(allocateKey, allocationRoutedKey, client, newSettings.build());
//        AllocationRoutedStep routedCheckStep = new AllocationRoutedStep(allocationRoutedKey, nextStepKey);
//        return Arrays.asList(allocateStep, routedCheckStep);
        return Arrays.asList();
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
