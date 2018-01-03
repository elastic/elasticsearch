/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 * A {@link LifecycleAction} which force-merges the index.
 */
public class ForceMergeAction implements LifecycleAction {
    public static final String NAME = "forcemerge";
    public static final ParseField MAX_NUM_SEGMENTS_FIELD = new ParseField("max_num_segments");

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        false, a -> {
        int maxNumSegments = (Integer) a[0];
        return new ForceMergeAction(maxNumSegments);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
    }

    private final Integer maxNumSegments;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName()
                + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        this.maxNumSegments = in.readVInt();
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxNumSegments);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        builder.endObject();
        return builder;
    }

    /**
     * Helper method to check if a force-merge is necessary based on {@code maxNumSegments} and then calls
     * the next {@code action}.
     *
     * @param index The specific index to check the segments of
     * @param client The client to execute the transport actions
     * @param listener The listener to call onFailure on if an exception occurs when executing the {@link IndicesSegmentsRequest}
     * @param nextAction The next action to execute if there are too many segments and force-merge is appropriate
     * @param skipToAction The next action to execute if there aren't too many segments
     */
    void checkSegments(Index index, Client client, Listener listener, Consumer<ActionResponse> nextAction,
                       Consumer<ActionResponse> skipToAction) {
        client.admin().indices().segments(new IndicesSegmentsRequest(index.getName()), ActionListener.wrap(r -> {
            boolean hasTooManySegments = StreamSupport.stream(r.getIndices().get(index.getName()).spliterator(), false)
                .anyMatch(iss -> Arrays.stream(iss.getShards()).anyMatch(p -> p.getSegments().size() > maxNumSegments));
            if (nextAction != null && hasTooManySegments && RestStatus.OK.equals(r.getStatus())) {
                nextAction.accept(r);
            } else {
                skipToAction.accept(r);
            }
        }, listener::onFailure));

    }

    /**
     * Helper method to execute the force-merge
     *
     * @param index The specific index to force-merge
     * @param client The client to execute the transport actions
     * @param listener The listener to call onFailure on if an exception occurs when executing the {@link ForceMergeRequest}
     * @param nextAction The next action to execute if the force-merge is successful
     */
    void forceMerge(Index index, Client client, Listener listener, Consumer<ActionResponse> nextAction) {
        ForceMergeRequest forceMergeRequest = new ForceMergeRequest(index.getName()).maxNumSegments(maxNumSegments);
        client.admin().indices().forceMerge(forceMergeRequest, ActionListener.wrap(r -> {
            if (RestStatus.OK.equals(r.getStatus())) {
                nextAction.accept(r);
            }
        }, listener::onFailure));

    }

    /**
     * Helper method to prepare the index for force-merging by making it read-only
     *
     * @param index The specific index to set as read-only
     * @param client The client to execute the transport actions
     * @param listener The listener to call onFailure on if an exception occurs when executing the {@link UpdateSettingsRequest}
     * @param nextAction The next action to execute if updating the setting is successful
     */
    void updateBlockWriteSettingToReadOnly(Index index, Client client, Listener listener, Consumer<ActionResponse> nextAction) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(Settings.builder()
            .put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build(), index.getName());
        client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(response -> {
            nextAction.accept(response);
        }, listener::onFailure));
    }

    /**
     * Helper method to return the index back to read-write mode since force-merging was successful
     *
     * @param index The specific index to set back as read-write
     * @param client The client to execute the transport actions
     * @param listener The listener to return a final response to for this {@link ForceMergeAction}.
     */
    void updateBlockWriteSettingToReadWrite(Index index, Client client, Listener listener) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(Settings.builder()
            .put(IndexMetaData.SETTING_BLOCKS_WRITE, false).build(), index.getName());
        client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(
            response -> listener.onSuccess(true), listener::onFailure));
    }

    @Override
    public void execute(Index index, Client client, ClusterService clusterService, Listener listener) {
        boolean isReadOnly = clusterService.state().metaData().indices().get(index.getName()).getSettings()
            .getAsBoolean(IndexMetaData.SETTING_BLOCKS_WRITE, false);
        if (isReadOnly) {
            // index is already read-only, so just check if a force-merge is necessary and set back
            // to read-write whether a force-merge is necessary or not.
            checkSegments(index, client, listener, r1 ->
                forceMerge(index, client, listener,
                    // after a successful force-merge, return the index to read-write
                    r2 -> updateBlockWriteSettingToReadWrite(index, client, listener)),
                r3 -> updateBlockWriteSettingToReadWrite(index, client, listener));
        } else {
            // first check if a force-merge is appropriate
            checkSegments(index, client, listener,
                // if appropriate, set the index to read-only
                r1 -> updateBlockWriteSettingToReadOnly(index, client, listener,
                    // once the index is read-only, run a force-merge on it
                    r2 -> forceMerge(index, client, listener,
                        // after a successful force-merge, return the index to read-write
                        r3 -> updateBlockWriteSettingToReadWrite(index, client, listener))),
                r4 -> { if (isReadOnly) updateBlockWriteSettingToReadWrite(index, client, listener); });
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ForceMergeAction other = (ForceMergeAction) obj;
        return Objects.equals(maxNumSegments, other.maxNumSegments);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
