/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class Parameters implements Writeable, ToXContentObject {

    public static final ParseField N_NEIGHBORS = new ParseField("n_neighbors");
    public static final ParseField METHOD = new ParseField("method");
    public static final ParseField FEATURE_INFLUENCE_THRESHOLD = new ParseField("feature_influence_threshold");
    public static final ParseField COMPUTE_FEATURE_INFLUENCE = new ParseField("compute_feature_influence");
    public static final ParseField OUTLIER_FRACTION = new ParseField("outlier_fraction");
    public static final ParseField STANDARDIZATION_ENABLED = new ParseField("standardization_enabled");

    public static Parameters fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return createParser(ignoreUnknownFields).apply(parser, null);
    }

    private static ConstructingObjectParser<Parameters, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Parameters, Void> parser = new ConstructingObjectParser<>(
            "outlier_detection_parameters",
            ignoreUnknownFields,
            a -> new Parameters((int) a[0], (String) a[1], (boolean) a[2], (double) a[3], (double) a[4], (boolean) a[5])
        );

        parser.declareInt(constructorArg(), N_NEIGHBORS);
        parser.declareString(constructorArg(), METHOD);
        parser.declareBoolean(constructorArg(), COMPUTE_FEATURE_INFLUENCE);
        parser.declareDouble(constructorArg(), FEATURE_INFLUENCE_THRESHOLD);
        parser.declareDouble(constructorArg(), OUTLIER_FRACTION);
        parser.declareBoolean(constructorArg(), STANDARDIZATION_ENABLED);

        return parser;
    }

    private final int nNeighbors;
    private final String method;
    private final boolean computeFeatureInfluence;
    private final double featureInfluenceThreshold;
    private final double outlierFraction;
    private final boolean standardizationEnabled;

    public Parameters(
        int nNeighbors,
        String method,
        boolean computeFeatureInfluence,
        double featureInfluenceThreshold,
        double outlierFraction,
        boolean standardizationEnabled
    ) {
        this.nNeighbors = nNeighbors;
        this.method = method;
        this.computeFeatureInfluence = computeFeatureInfluence;
        this.featureInfluenceThreshold = featureInfluenceThreshold;
        this.outlierFraction = outlierFraction;
        this.standardizationEnabled = standardizationEnabled;
    }

    public Parameters(StreamInput in) throws IOException {
        this.nNeighbors = in.readVInt();
        this.method = in.readString();
        this.computeFeatureInfluence = in.readBoolean();
        this.featureInfluenceThreshold = in.readDouble();
        this.outlierFraction = in.readDouble();
        this.standardizationEnabled = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(nNeighbors);
        out.writeString(method);
        out.writeBoolean(computeFeatureInfluence);
        out.writeDouble(featureInfluenceThreshold);
        out.writeDouble(outlierFraction);
        out.writeBoolean(standardizationEnabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(N_NEIGHBORS.getPreferredName(), nNeighbors);
        builder.field(METHOD.getPreferredName(), method);
        builder.field(COMPUTE_FEATURE_INFLUENCE.getPreferredName(), computeFeatureInfluence);
        builder.field(FEATURE_INFLUENCE_THRESHOLD.getPreferredName(), featureInfluenceThreshold);
        builder.field(OUTLIER_FRACTION.getPreferredName(), outlierFraction);
        builder.field(STANDARDIZATION_ENABLED.getPreferredName(), standardizationEnabled);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Parameters that = (Parameters) o;
        return nNeighbors == that.nNeighbors
            && Objects.equals(method, that.method)
            && computeFeatureInfluence == that.computeFeatureInfluence
            && featureInfluenceThreshold == that.featureInfluenceThreshold
            && outlierFraction == that.outlierFraction
            && standardizationEnabled == that.standardizationEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nNeighbors,
            method,
            computeFeatureInfluence,
            featureInfluenceThreshold,
            outlierFraction,
            standardizationEnabled
        );
    }
}
