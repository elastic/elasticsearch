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

public abstract class TierMetrics implements ToXContentObject, Writeable {

    public static class IndexTierMetrics extends TierMetrics {

        public IndexTierMetrics(final StreamInput input) throws IOException {
            super(input);
        }

        public IndexTierMetrics(MetricsContainer metrics, ConstraintsContainer constraints) {
            super(metrics, constraints);
        }

    }

    public static class SearchTierMetrics extends TierMetrics {

        public SearchTierMetrics(final StreamInput input) throws IOException {
            super(input);
        }

        public SearchTierMetrics(MetricsContainer metrics, ConstraintsContainer constraints) {
            super(metrics, constraints);
        }

    }

    private final MetricsContainer metrics;
    private final ConstraintsContainer constraints;

    public TierMetrics(final MetricsContainer metrics, final ConstraintsContainer constraints) {
        this.metrics = metrics;
        this.constraints = constraints;
    }

    public TierMetrics(StreamInput input) throws IOException {
        this.metrics = new MetricsContainer(input);
        this.constraints = new ConstraintsContainer(input);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("metrics", metrics);
        builder.field("constraints", constraints);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        metrics.writeTo(out);
        constraints.writeTo(out);
    }
}
