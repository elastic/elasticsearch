/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.model;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class StatelessAutoscalingMetrics implements ToXContentObject, Writeable {

    private final TierMetrics indexTierMetrics;
    private final TierMetrics searchTierMetrics;
    private final TierMetrics mlTierMetrics;

    public StatelessAutoscalingMetrics(final StreamInput input) throws IOException {
        this.indexTierMetrics = new TierMetrics.IndexTierMetrics(input);
        this.searchTierMetrics = new TierMetrics.SearchTierMetrics(input);
        this.mlTierMetrics = new TierMetrics.MlTierMetrics(input);
    }

    public StatelessAutoscalingMetrics(
        final TierMetrics indexTierMetrics,
        final TierMetrics searchTierMetrics,
        final TierMetrics mlTierMetrics
    ) {
        this.indexTierMetrics = indexTierMetrics;
        this.searchTierMetrics = searchTierMetrics;
        this.mlTierMetrics = mlTierMetrics;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index-tier", indexTierMetrics);
        builder.field("search-tier", searchTierMetrics);
        builder.field("ml-tier", mlTierMetrics);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indexTierMetrics.writeTo(out);
        searchTierMetrics.writeTo(out);
        mlTierMetrics.writeTo(out);
    }
}
