/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.querysampling.estimate.RecallEstimate;

import java.io.IOException;
import java.util.List;

public final class QuerySamplingRecallResponse extends ActionResponse implements ToXContentObject {

    public record Sample(
        @Nullable String label,
        boolean event,
        long multiplicity,
        double weightedMultiplicity,
        double inclusionProbability,
        double seenProbability,
        @Nullable Double recall
    ) {}

    private final RecallEstimate estimate;
    private final List<Sample> samples;

    public QuerySamplingRecallResponse(RecallEstimate estimate, @Nullable List<Sample> samples) {
        this.estimate = estimate;
        this.samples = samples;
    }

    public RecallEstimate estimate() {
        return estimate;
    }

    @Nullable
    public List<Sample> samples() {
        return samples;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    private static void groups(XContentBuilder builder, String name, String keyName, List<RecallEstimate.GroupEstimate> groups)
        throws IOException {
        builder.startArray(name);
        for (RecallEstimate.GroupEstimate group : groups) {
            builder.startObject();
            builder.field(keyName, group.key());
            builder.field("records_with_ground_truth", group.recordsWithGroundTruth());
            builder.field("traffic_weighted_recall", group.trafficWeightedRecall());
            builder.field("traffic_effective_size", group.trafficEffectiveSize());
            builder.field("unique_query_recall", group.uniqueQueryRecall());
            builder.field("unique_query_effective_size", group.uniqueQueryEffectiveSize());
            builder.endObject();
        }
        builder.endArray();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("records", estimate.records());
        builder.field("records_with_ground_truth", estimate.recordsWithGroundTruth());
        builder.field("traffic_weighted_recall", estimate.trafficWeightedRecall());
        builder.field("traffic_effective_size", estimate.trafficEffectiveSize());
        builder.field("unique_query_recall", estimate.uniqueQueryRecall());
        builder.field("unique_query_effective_size", estimate.uniqueQueryEffectiveSize());
        groups(builder, "by_hardness", "hardness", estimate.byHardness());
        groups(builder, "by_cluster", "cluster", estimate.byCluster());
        groups(builder, "by_selectivity", "selectivity", estimate.bySelectivity());
        builder.startObject("event_slice");
        builder.field("records", estimate.events().records());
        builder.field("records_with_ground_truth", estimate.events().recordsWithGroundTruth());
        builder.field("recall", estimate.events().recall());
        builder.field("effective_size", estimate.events().effectiveSize());
        builder.endObject();
        if (samples != null) {
            builder.startArray("samples");
            for (Sample sample : samples) {
                builder.startObject();
                builder.field("label", sample.label());
                builder.field("event", sample.event());
                builder.field("multiplicity", sample.multiplicity());
                builder.field("weighted_multiplicity", sample.weightedMultiplicity());
                builder.field("inclusion_probability", sample.inclusionProbability());
                builder.field("seen_probability", sample.seenProbability());
                builder.field("recall", sample.recall());
                builder.endObject();
            }
            builder.endArray();
        }
        return builder.endObject();
    }
}
