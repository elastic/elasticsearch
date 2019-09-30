/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WeightedSum implements StrictlyParsedOutputAggregator, LenientlyParsedOutputAggregator {

    public static final ParseField NAME = new ParseField("weighted_sum");
    public static final ParseField WEIGHTS = new ParseField("weights");

    private static final ConstructingObjectParser<WeightedSum, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<WeightedSum, Void> STRICT_PARSER = createParser(false);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<WeightedSum, Void> createParser(boolean lenient) {
        ConstructingObjectParser<WeightedSum, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new WeightedSum((List<Double>)a[0]));
        parser.declareDoubleArray(ConstructingObjectParser.constructorArg(), WEIGHTS);
        return parser;
    }

    public static WeightedSum fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null);
    }

    public static WeightedSum fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null);
    }

    private final List<Double> weights;

    public WeightedSum(List<Double> weights) {
        this.weights = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(weights, WEIGHTS.getPreferredName()));
    }

    public WeightedSum(StreamInput in) throws IOException {
        this.weights = Collections.unmodifiableList(in.readList(StreamInput::readDouble));
    }

    @Override
    public List<Double> processValues(List<Double> values) {
        Objects.requireNonNull(values, "values must not be null");
        if (values.size() != weights.size()) {
            throw new IllegalArgumentException("values must be the same length as weights.");
        }
        return IntStream.range(0, weights.size()).mapToDouble(i -> values.get(i) * weights.get(i)).boxed().collect(Collectors.toList());
    }

    @Override
    public double aggregate(List<Double> values) {
        Objects.requireNonNull(values, "values must not be null");
        Optional<Double> summation = values.stream().reduce((memo, v) -> memo + v);
        if (summation.isPresent()) {
            return summation.get();
        }
        throw new IllegalArgumentException("values must not contain null values");
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(weights, StreamOutput::writeDouble);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(WEIGHTS.getPreferredName(), weights);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedSum that = (WeightedSum) o;
        return Objects.equals(weights, that.weights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weights);
    }

    @Override
    public Integer expectedValueSize() {
        return this.weights.size();
    }
}
