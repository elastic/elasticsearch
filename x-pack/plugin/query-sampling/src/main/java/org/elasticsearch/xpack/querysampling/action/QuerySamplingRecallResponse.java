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

/**
 * The estimate, and optionally what each stored query contributed to it. Like the request it stays on the node that
 * made it.
 */
public final class QuerySamplingRecallResponse extends ActionResponse implements ToXContentObject {

    /**
     * What one stored query contributed.
     *
     * @param label                the {@code X-Opaque-Id} of the search that was picked, which is only a label
     * @param multiplicity         how many times the query was captured
     * @param weightedMultiplicity the estimated number of times it was searched
     * @param inclusionProbability chance of the query to be picked
     * @param seenProbability      chance of the query to have been captured at all
     * @param recall               its recall, or {@code null} while its ground truth is not known
     */
    public record Sample(
        @Nullable String label,
        long multiplicity,
        double weightedMultiplicity,
        double inclusionProbability,
        double seenProbability,
        @Nullable Double recall
    ) {}

    private final RecallEstimate estimate;
    private final List<Sample> samples;

    /**
     * @param samples what each query contributed, or {@code null} if it was not asked for
     */
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("records", estimate.records());
        builder.field("records_with_ground_truth", estimate.recordsWithGroundTruth());
        builder.field("traffic_weighted_recall", estimate.trafficWeightedRecall());
        builder.field("traffic_effective_size", estimate.trafficEffectiveSize());
        builder.field("unique_query_recall", estimate.uniqueQueryRecall());
        builder.field("unique_query_effective_size", estimate.uniqueQueryEffectiveSize());
        if (samples != null) {
            builder.startArray("samples");
            for (Sample sample : samples) {
                builder.startObject();
                builder.field("label", sample.label());
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
