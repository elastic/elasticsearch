/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class BucketMetricsPipelineAggregationBuilder<AF extends BucketMetricsPipelineAggregationBuilder<AF>> extends
    AbstractPipelineAggregationBuilder<AF> {

    private String format;
    private GapPolicy gapPolicy;

    protected BucketMetricsPipelineAggregationBuilder(String name, String type, String[] bucketsPaths) {
        this(name, type, bucketsPaths, null, GapPolicy.SKIP);
    }

    protected BucketMetricsPipelineAggregationBuilder(String name, String type, String[] bucketsPaths, String format, GapPolicy gapPolicy) {
        super(name, type, bucketsPaths);
        this.format = format;
        this.gapPolicy = gapPolicy;
    }

    /**
     * Read from a stream.
     */
    protected BucketMetricsPipelineAggregationBuilder(StreamInput in, String type) throws IOException {
        super(in, type);
        format = in.readOptionalString();
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalString(format);
        gapPolicy.writeTo(out);
        innerWriteTo(out);
    }

    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    /**
     * Sets the format to use on the output of this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AF format(String format) {
        this.format = format;
        return (AF) this;
    }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return DocValueFormat.RAW;
        }
    }

    /**
     * Sets the gap policy to use for this aggregation.
     */
    @SuppressWarnings("unchecked")
    public AF gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return (AF) this;
    }

    /**
     * Gets the gap policy to use for this aggregation.
     */
    public GapPolicy gapPolicy() {
        return gapPolicy;
    }

    @Override
    protected abstract PipelineAggregator createInternal(Map<String, Object> metadata);

    @Override
    protected void validate(ValidationContext context) {
        if (bucketsPaths.length != 1) {
            context.addBucketPathValidationError("must contain a single entry for aggregation [" + name + "]");
            return;
        }
        // find the first agg name in the buckets path to check its a multi bucket agg
        List<AggregationPath.PathElement> path = AggregationPath.parse(bucketsPaths[0]).getPathElements();
        int pathPos = 0;
        AggregationPath.PathElement currentAgg = path.get(pathPos++);
        final String aggName = currentAgg.name();
        Optional<AggregationBuilder> aggBuilder = context.getSiblingAggregations()
            .stream()
            .filter(builder -> builder.getName().equals(aggName))
            .findAny();
        if (aggBuilder.isEmpty()) {
            context.addBucketPathValidationError("aggregation does not exist for aggregation [" + name + "]: " + bucketsPaths[0]);
            return;
        }

        // Dig through the aggregation tree to find the first aggregation specified by the path.
        // The path may have many single bucket aggs (with sub-aggs) or many multi-bucket aggs specified by bucket keys
        while (aggBuilder.isPresent()
            && pathPos < path.size()
            && ((aggBuilder.get().bucketCardinality() == AggregationBuilder.BucketCardinality.MANY
                && AggregationPath.pathElementContainsBucketKey(currentAgg))
                || (aggBuilder.get().bucketCardinality() == AggregationBuilder.BucketCardinality.ONE
                    && aggBuilder.get().getSubAggregations().isEmpty() == false))) {
            currentAgg = path.get(pathPos++);
            final String subAggName = currentAgg.name();
            aggBuilder = aggBuilder.get().getSubAggregations().stream().filter(b -> b.getName().equals(subAggName)).findAny();
        }
        if (aggBuilder.isEmpty()) {
            context.addBucketPathValidationError(
                "aggregation does not exist for aggregation ["
                    + name
                    + "]: "
                    + AggregationPath.pathElementsAsStringList(path.subList(0, pathPos))
            );
            return;
        }
        if (aggBuilder.get().bucketCardinality() != AggregationBuilder.BucketCardinality.MANY) {
            context.addValidationError(
                "Unable to find unqualified multi-bucket aggregation in "
                    + PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName()
                    + ". Path must include a multi-bucket aggregation for aggregation ["
                    + name
                    + "] found :"
                    + aggBuilder.get().getClass().getName()
                    + " for buckets path: "
                    + bucketsPaths[0]
            );
        }
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(BucketMetricsParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(BucketMetricsParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        doXContentBody(builder, params);
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), format, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        @SuppressWarnings("unchecked")
        BucketMetricsPipelineAggregationBuilder<AF> other = (BucketMetricsPipelineAggregationBuilder<AF>) obj;
        return Objects.equals(format, other.format) && Objects.equals(gapPolicy, other.gapPolicy);
    }

}
