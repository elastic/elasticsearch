/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.ensemble;


import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;


public class WeightedMode implements OutputAggregator {

    public static final String NAME = "weighted_mode";
    public static final ParseField WEIGHTS = new ParseField("weights");
    public static final ParseField NUM_CLASSES = new ParseField("num_classes");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<WeightedMode, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new WeightedMode((Integer)a[0], (List<Double>)a[1]));
    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUM_CLASSES);
        PARSER.declareDoubleArray(ConstructingObjectParser.optionalConstructorArg(), WEIGHTS);
    }

    public static WeightedMode fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final List<Double> weights;
    private final int numClasses;

    public WeightedMode(int numClasses, List<Double> weights) {
        this.weights = weights;
        this.numClasses = numClasses;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (weights != null) {
            builder.field(WEIGHTS.getPreferredName(), weights);
        }
        builder.field(NUM_CLASSES.getPreferredName(), numClasses);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedMode that = (WeightedMode) o;
        return Objects.equals(weights, that.weights) && numClasses == that.numClasses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(weights, numClasses);
    }
}
