/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
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

    @Override
    public List<Step> toSteps(String phase) {
//        ClusterStateUpdateStep readOnlyStep = new ClusterStateUpdateStep(
//            "read_only", NAME, phase, index.getName(), (currentState) -> {
//            Settings readOnlySettings = Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build();
//            return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
//                .updateSettings(readOnlySettings, index.getName())).build();
//        });
//
//        ClientStep<IndicesSegmentsRequestBuilder, IndicesSegmentResponse> segmentCount = new ClientStep<>( "segment_count",
//            NAME, phase, index.getName(),
//            client.admin().indices().prepareSegments(index.getName()),
//            currentState -> false, response -> {
//                // check if has too many segments
//                return StreamSupport.stream(response.getIndices().get(index.getName()).spliterator(), false)
//                    .anyMatch(iss -> Arrays.stream(iss.getShards()).anyMatch(p -> p.getSegments().size() > maxNumSegments));
//        });
//
//        ClientStep forceMerge = new ClientStep<ForceMergeRequestBuilder, ForceMergeResponse>( "force_merge",
//            NAME, phase, index.getName(),
//            client.admin().indices().prepareForceMerge(index.getName()).setMaxNumSegments(maxNumSegments),
//            currentState -> false, response -> RestStatus.OK.equals(response.getStatus()));
//
//        ClusterStateUpdateStep readWriteStep = new ClusterStateUpdateStep(
//            "read_only", NAME, phase, index.getName(), (currentState) -> {
//            Settings readOnlySettings = Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, false).build();
//            return ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
//                .updateSettings(readOnlySettings, index.getName())).build();
//        });

        return Arrays.asList();
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
