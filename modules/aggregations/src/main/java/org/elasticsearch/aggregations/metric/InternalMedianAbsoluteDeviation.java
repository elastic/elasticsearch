/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aggregations.metric;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An aggregation that approximates the median absolute deviation of a numeric field
 *
 * @see <a href="https://en.wikipedia.org/wiki/Median_absolute_deviation">https://en.wikipedia.org/wiki/Median_absolute_deviation</a>
 */
public class InternalMedianAbsoluteDeviation extends InternalNumericMetricsAggregation.SingleValue {

    static double computeMedianAbsoluteDeviation(TDigestState valuesSketch) {

        if (valuesSketch.size() == 0) {
            return Double.NaN;
        } else {
            final double approximateMedian = valuesSketch.quantile(0.5);
            final TDigestState approximatedDeviationsSketch = new TDigestState(valuesSketch.compression());
            valuesSketch.centroids().forEach(centroid -> {
                final double deviation = Math.abs(approximateMedian - centroid.mean());
                approximatedDeviationsSketch.add(deviation, centroid.count());
            });

            return approximatedDeviationsSketch.quantile(0.5);
        }
    }

    private final TDigestState valuesSketch;
    private final double medianAbsoluteDeviation;

    InternalMedianAbsoluteDeviation(String name, Map<String, Object> metadata, DocValueFormat format, TDigestState valuesSketch) {
        super(name, Objects.requireNonNull(format), metadata);
        this.valuesSketch = Objects.requireNonNull(valuesSketch);

        this.medianAbsoluteDeviation = computeMedianAbsoluteDeviation(this.valuesSketch);
    }

    public InternalMedianAbsoluteDeviation(StreamInput in) throws IOException {
        super(in);
        valuesSketch = TDigestState.read(in);
        medianAbsoluteDeviation = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        TDigestState.write(valuesSketch, out);
        out.writeDouble(medianAbsoluteDeviation);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        final TDigestState valueMerged = new TDigestState(valuesSketch.compression());
        for (InternalAggregation aggregation : aggregations) {
            final InternalMedianAbsoluteDeviation madAggregation = (InternalMedianAbsoluteDeviation) aggregation;
            valueMerged.add(madAggregation.valuesSketch);
        }

        return new InternalMedianAbsoluteDeviation(name, metadata, format, valueMerged);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean anyResults = valuesSketch.size() > 0;
        final Double mad = anyResults ? medianAbsoluteDeviation : null;

        builder.field(CommonFields.VALUE.getPreferredName(), mad);
        if (format != DocValueFormat.RAW && anyResults) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(mad).toString());
        }

        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valuesSketch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalMedianAbsoluteDeviation other = (InternalMedianAbsoluteDeviation) obj;
        return Objects.equals(valuesSketch, other.valuesSketch);
    }

    @Override
    public String getWriteableName() {
        return MedianAbsoluteDeviationAggregationBuilder.NAME;
    }

    TDigestState getValuesSketch() {
        return valuesSketch;
    }

    @Override
    public double value() {
        return medianAbsoluteDeviation;
    }

}
