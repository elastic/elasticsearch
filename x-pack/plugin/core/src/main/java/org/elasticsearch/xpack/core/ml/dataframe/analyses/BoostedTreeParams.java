/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.AbstractObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parameters used by both {@link Classification} and {@link Regression} analyses.
 */
public class BoostedTreeParams implements ToXContentFragment, Writeable {

    static final String NAME = "boosted_tree_params";

    public static final ParseField LAMBDA = new ParseField("lambda");
    public static final ParseField GAMMA = new ParseField("gamma");
    public static final ParseField ETA = new ParseField("eta");
    public static final ParseField MAXIMUM_NUMBER_TREES = new ParseField("maximum_number_trees");
    public static final ParseField FEATURE_BAG_FRACTION = new ParseField("feature_bag_fraction");

    static void declareFields(AbstractObjectParser<?, Void> parser) {
        parser.declareDouble(optionalConstructorArg(), LAMBDA);
        parser.declareDouble(optionalConstructorArg(), GAMMA);
        parser.declareDouble(optionalConstructorArg(), ETA);
        parser.declareInt(optionalConstructorArg(), MAXIMUM_NUMBER_TREES);
        parser.declareDouble(optionalConstructorArg(), FEATURE_BAG_FRACTION);
    }

    private final Double lambda;
    private final Double gamma;
    private final Double eta;
    private final Integer maximumNumberTrees;
    private final Double featureBagFraction;

    BoostedTreeParams(@Nullable Double lambda,
                      @Nullable Double gamma,
                      @Nullable Double eta,
                      @Nullable Integer maximumNumberTrees,
                      @Nullable Double featureBagFraction) {
        if (lambda != null && lambda < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", LAMBDA.getPreferredName());
        }
        if (gamma != null && gamma < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", GAMMA.getPreferredName());
        }
        if (eta != null && (eta < 0.001 || eta > 1)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [0.001, 1]", ETA.getPreferredName());
        }
        if (maximumNumberTrees != null && (maximumNumberTrees <= 0 || maximumNumberTrees > 2000)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, 2000]", MAXIMUM_NUMBER_TREES.getPreferredName());
        }
        if (featureBagFraction != null && (featureBagFraction <= 0 || featureBagFraction > 1.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in (0, 1]", FEATURE_BAG_FRACTION.getPreferredName());
        }
        this.lambda = lambda;
        this.gamma = gamma;
        this.eta = eta;
        this.maximumNumberTrees = maximumNumberTrees;
        this.featureBagFraction = featureBagFraction;
    }

    BoostedTreeParams() {
        this(null, null, null, null, null);
    }

    BoostedTreeParams(StreamInput in) throws IOException {
        lambda = in.readOptionalDouble();
        gamma = in.readOptionalDouble();
        eta = in.readOptionalDouble();
        maximumNumberTrees = in.readOptionalVInt();
        featureBagFraction = in.readOptionalDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(lambda);
        out.writeOptionalDouble(gamma);
        out.writeOptionalDouble(eta);
        out.writeOptionalVInt(maximumNumberTrees);
        out.writeOptionalDouble(featureBagFraction);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (lambda != null) {
            builder.field(LAMBDA.getPreferredName(), lambda);
        }
        if (gamma != null) {
            builder.field(GAMMA.getPreferredName(), gamma);
        }
        if (eta != null) {
            builder.field(ETA.getPreferredName(), eta);
        }
        if (maximumNumberTrees != null) {
            builder.field(MAXIMUM_NUMBER_TREES.getPreferredName(), maximumNumberTrees);
        }
        if (featureBagFraction != null) {
            builder.field(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
        return builder;
    }

    Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        if (lambda != null) {
            params.put(LAMBDA.getPreferredName(), lambda);
        }
        if (gamma != null) {
            params.put(GAMMA.getPreferredName(), gamma);
        }
        if (eta != null) {
            params.put(ETA.getPreferredName(), eta);
        }
        if (maximumNumberTrees != null) {
            params.put(MAXIMUM_NUMBER_TREES.getPreferredName(), maximumNumberTrees);
        }
        if (featureBagFraction != null) {
            params.put(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoostedTreeParams that = (BoostedTreeParams) o;
        return Objects.equals(lambda, that.lambda)
            && Objects.equals(gamma, that.gamma)
            && Objects.equals(eta, that.eta)
            && Objects.equals(maximumNumberTrees, that.maximumNumberTrees)
            && Objects.equals(featureBagFraction, that.featureBagFraction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lambda, gamma, eta, maximumNumberTrees, featureBagFraction);
    }
}
