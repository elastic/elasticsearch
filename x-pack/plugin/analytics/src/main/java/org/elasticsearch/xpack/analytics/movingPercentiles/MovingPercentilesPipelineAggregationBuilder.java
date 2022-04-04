/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.analytics.movingPercentiles;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class MovingPercentilesPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<
    MovingPercentilesPipelineAggregationBuilder> {
    public static final String NAME = "moving_percentiles";
    private static final ParseField WINDOW = new ParseField("window");
    private static final ParseField SHIFT = new ParseField("shift");

    public static final ConstructingObjectParser<MovingPercentilesPipelineAggregationBuilder, String> PARSER =
        new ConstructingObjectParser<>(
            NAME,
            false,
            (args, name) -> { return new MovingPercentilesPipelineAggregationBuilder(name, (String) args[0], (int) args[1]); }
        );
    static {
        PARSER.declareString(constructorArg(), BUCKETS_PATH_FIELD);
        PARSER.declareInt(constructorArg(), WINDOW);
        PARSER.declareInt(MovingPercentilesPipelineAggregationBuilder::setShift, SHIFT);
    }

    private final int window;
    private int shift;

    public MovingPercentilesPipelineAggregationBuilder(String name, String bucketsPath, int window) {
        super(name, NAME, new String[] { bucketsPath });
        if (window <= 0) {
            throw new IllegalArgumentException("[" + WINDOW.getPreferredName() + "] must be a positive, non-zero integer.");
        }
        this.window = window;
    }

    /**
     * Read from a stream.
     */
    public MovingPercentilesPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        window = in.readVInt();
        shift = in.readInt();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(window);
        out.writeInt(shift);
    }

    /**
     * Returns the window size for this aggregation
     */
    public int getWindow() {
        return window;
    }

    /**
     * Returns the shift for this aggregation
     */
    public int getShift() {
        return shift;
    }

    /**
     * Sets the shift for this aggregation
     */
    public void setShift(int shift) {
        this.shift = shift;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        return new MovingPercentilesPipelineAggregator(name, bucketsPaths, getWindow(), getShift(), metaData);
    }

    @Override
    protected void validate(ValidationContext context) {
        if (bucketsPaths.length != 1) {
            context.addBucketPathValidationError("must contain a single entry for aggregation [" + name + "]");
        }
        context.validateParentAggSequentiallyOrdered(NAME, name);
    }

    @Override
    protected final XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPaths[0]);
        builder.field(WINDOW.getPreferredName(), window);
        builder.field(SHIFT.getPreferredName(), shift);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), window, shift);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        MovingPercentilesPipelineAggregationBuilder other = (MovingPercentilesPipelineAggregationBuilder) obj;
        return window == other.window && shift == other.shift;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_9_0;
    }
}
