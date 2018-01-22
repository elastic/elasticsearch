/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

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

    /**
     * Executes the Shrink Action.
     *
     * This function first checks whether the target shrunk index exists already, if it does not, then
     * it will set the index to read-only and issue a resize request.
     *
     * Since the shrink response is not returned after a successful shrunk operation, we must poll to see if
     * all the shards of the newly shrunk index are initialized. If so, then we can return the index to read-write
     * and tell the listener that we have completed the action.
     *
     * @param index
     *            the {@link Index} on which to perform the action.
     * @param client
     *            the {@link Client} to use for making changes to the index.
     * @param clusterService
     *            the {@link ClusterService} to retrieve the current cluster state from.
     * @param listener
     *            the {@link LifecycleAction.Listener} to return completion or failure responses to.
     */
    @Override
    public void execute(Index index, Client client, ClusterService clusterService, Listener listener) {
        String targetIndexName = SHRUNK_INDEX_NAME_PREFIX + index.getName();
        ClusterState clusterState = clusterService.state();
        IndexMetaData indexMetaData = clusterState.metaData().index(index.getName());
        String sourceIndexName = IndexMetaData.INDEX_SHRINK_SOURCE_NAME.get(indexMetaData.getSettings());
        boolean isShrunkIndex = index.getName().equals(SHRUNK_INDEX_NAME_PREFIX + sourceIndexName);
        IndexMetaData shrunkIndexMetaData = clusterState.metaData().index(targetIndexName);
        if (isShrunkIndex) {
            // We are currently managing the shrunken index. This means all previous operations were successful and
            // the original index is deleted. It is important to add an alias from the original index name to the shrunken
            // index so that previous actions will still succeed.
            boolean aliasAlreadyExists = indexMetaData.getAliases().values().contains(AliasMetaData.builder(sourceIndexName).build());
            boolean sourceIndexDeleted = clusterState.metaData().hasIndex(sourceIndexName) == false;
            if (sourceIndexDeleted && aliasAlreadyExists) {
                listener.onSuccess(true);
            } else {
                IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest()
                    .addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(sourceIndexName))
                    .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index.getName()).alias(sourceIndexName));
                client.admin().indices().aliases(aliasesRequest, ActionListener.wrap(response -> {
                    listener.onSuccess(true);
                }, listener::onFailure));
            }
        } else if (shrunkIndexMetaData == null) {
            // Shrunken index is not present yet, it is time to issue to shrink request
            ResizeRequest resizeRequest = new ResizeRequest(targetIndexName, index.getName());
            resizeRequest.getTargetIndexRequest().settings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, indexMetaData.getNumberOfReplicas())
                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, indexMetaData.getCreationDate())
                .build());
            indexMetaData.getAliases().values().spliterator().forEachRemaining(aliasMetaDataObjectCursor -> {
                resizeRequest.getTargetIndexRequest().alias(new Alias(aliasMetaDataObjectCursor.value.alias()));
            });
            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(Settings.builder()
                .put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build(), index.getName());
            client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(r -> {
                client.admin().indices().resizeIndex(resizeRequest, ActionListener.wrap(
                    resizeResponse -> {
                        if (resizeResponse.isAcknowledged()) {
                            listener.onSuccess(false);
                        } else {
                            listener.onFailure(new IllegalStateException("Shrink request failed to be acknowledged"));
                        }
                    }, listener::onFailure));
            }, listener::onFailure));
        } else if (index.getName().equals(IndexMetaData.INDEX_SHRINK_SOURCE_NAME.get(shrunkIndexMetaData.getSettings())) == false) {
            // The target shrunken index exists, but it was not shrunk from our managed index. This means
            // some external actions were done to create this index, and so we cannot progress with the shrink
            // action until this is resolved.
            listener.onFailure(new IllegalStateException("Cannot shrink index [" + index.getName() + "] because target " +
                "index [" + targetIndexName + "] already exists."));
        } else if (ActiveShardCount.ALL.enoughShardsActive(clusterService.state(), targetIndexName)) {
            if (indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME)
                    .equals(shrunkIndexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME))) {
                // Since both the shrunken and original indices co-exist, do nothing and wait until
                // the final step of the shrink action is completed and this original index is deleted.
                listener.onSuccess(false);
            } else {
                // Since all shards of the shrunken index are active, it is safe to continue forward
                // and begin swapping the indices by inheriting the lifecycle management to the new shrunken index.
                UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(Settings.builder()
                    .put(LifecycleSettings.LIFECYCLE_NAME, indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME))
                    .put(LifecycleSettings.LIFECYCLE_PHASE, indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_PHASE))
                    .put(LifecycleSettings.LIFECYCLE_ACTION, indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_ACTION)).build(), targetIndexName);
                client.admin().indices().updateSettings(updateSettingsRequest,
                    ActionListener.wrap(r -> listener.onSuccess(false) , listener::onFailure));
            }
        } else {
            // We are here because both the shrunken and original indices exist, but the shrunken index is not
            // fully active yet. This means that we wait for another poll iteration of execute to check the
            // state again.
            logger.debug("index [" + index.getName() + "] has been shrunk to shrunken-index [" + targetIndexName + "], but" +
                "shrunken index is not fully active yet");
            listener.onSuccess(false);
        }
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
