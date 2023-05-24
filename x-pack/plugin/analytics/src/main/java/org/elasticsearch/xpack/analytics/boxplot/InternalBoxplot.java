/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalBoxplot extends InternalNumericMetricsAggregation.MultiValue implements Boxplot {

    /**
     * This value is used in determining the width of the whiskers of the boxplot.  After the IQR value is calculated, it gets multiplied
     * by this multiplier to decide how far out from the q1 and q3 points to extend the whiskers.  The value of 1.5 is traditional.
     * See https://en.wikipedia.org/wiki/Box_plot
     */
    public static final double IQR_MULTIPLIER = 1.5;

    enum Metrics {
        MIN {
            @Override
            double value(InternalBoxplot boxplot) {
                return boxplot.getMin();
            }

            @Override
            double value(TDigestState digestState) {
                return digestState == null ? Double.NEGATIVE_INFINITY : digestState.getMin();
            }
        },
        MAX {
            @Override
            double value(InternalBoxplot boxplot) {
                return boxplot.getMax();
            }

            @Override
            double value(TDigestState digestState) {
                return digestState == null ? Double.POSITIVE_INFINITY : digestState.getMax();
            }
        },
        Q1 {
            @Override
            double value(InternalBoxplot boxplot) {
                return boxplot.getQ1();
            }

            @Override
            double value(TDigestState digestState) {
                return digestState == null ? Double.NaN : digestState.quantile(0.25);
            }
        },
        Q2 {
            @Override
            double value(InternalBoxplot boxplot) {
                return boxplot.getQ2();
            }

            @Override
            double value(TDigestState digestState) {
                return digestState == null ? Double.NaN : digestState.quantile(0.5);
            }
        },
        Q3 {
            @Override
            double value(InternalBoxplot boxplot) {
                return boxplot.getQ3();
            }

            @Override
            double value(TDigestState digestState) {
                return digestState == null ? Double.NaN : digestState.quantile(0.75);
            }
        },
        LOWER {
            @Override
            double value(InternalBoxplot boxplot) {
                return whiskers(boxplot.state)[0];
            }

            @Override
            double value(TDigestState digestState) {
                return whiskers(digestState)[0];
            }
        },
        UPPER {
            @Override
            double value(InternalBoxplot boxplot) {
                return whiskers(boxplot.state)[1];
            }

            @Override
            double value(TDigestState digestState) {
                return whiskers(digestState)[1];
            }
        };

        public static Metrics resolve(String name) {
            return Metrics.valueOf(name.toUpperCase(Locale.ROOT));
        }

        public static boolean hasMetric(String name) {
            try {
                InternalBoxplot.Metrics.resolve(name);
                return true;
            } catch (IllegalArgumentException iae) {
                return false;
            }
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        abstract double value(InternalBoxplot boxplot);

        abstract double value(TDigestState state);
    }

    /**
     * For a given TDigest, find the "whisker" valeus, such that the upper whisker is (close to) the highest observed value less than
     * q3 + 1.5 * IQR and the lower whisker is (close to) the lowest observed value greater than q1 - 1.5 * IQR.  Since we don't track
     * observed values directly, this function returns the centroid according to the above logic.
     *
     * @param state - an initialized TDigestState representing the observed data.
     * @return - two doubles in an array, where whiskers[0] is the lower whisker and whiskers[1] is the upper whisker.
     */
    public static double[] whiskers(TDigestState state) {
        double[] results = new double[2];
        results[0] = Double.NaN;
        results[1] = Double.NaN;
        if (state == null) {
            return results;
        }

        double q3 = state.quantile(0.75);
        double q1 = state.quantile(0.25);
        double iqr = q3 - q1;
        double upper = q3 + (IQR_MULTIPLIER * iqr);
        double lower = q1 - (IQR_MULTIPLIER * iqr);
        Centroid prev = null;
        // Does this iterate in ascending order? if not, we might need to sort...
        for (Centroid c : state.centroids()) {
            if (Double.isNaN(results[0]) && c.mean() > lower) {
                results[0] = c.mean();
            }
            if (c.mean() > upper) {
                results[1] = prev.mean();
                break;
            }
            prev = c;
        }
        if (Double.isNaN(results[1])) {
            results[1] = state.getMax();
        }
        return results;
    }

    static InternalBoxplot empty(String name, double compression, DocValueFormat format, Map<String, Object> metadata) {
        return new InternalBoxplot(name, new TDigestState(compression), format, metadata);
    }

    static final Set<String> METRIC_NAMES = Collections.unmodifiableSet(
        Stream.of(Metrics.values()).map(m -> m.name().toLowerCase(Locale.ROOT)).collect(Collectors.toSet())
    );

    private final TDigestState state;

    InternalBoxplot(String name, TDigestState state, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, formatter, metadata);
        this.state = state;
    }

    /**
     * Read from a stream.
     */
    public InternalBoxplot(StreamInput in) throws IOException {
        super(in);
        state = TDigestState.read(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        TDigestState.write(state, out);
    }

    @Override
    public String getWriteableName() {
        return BoxplotAggregationBuilder.NAME;
    }

    @Override
    public double getMin() {
        return state.getMin();
    }

    @Override
    public double getMax() {
        return state.getMax();
    }

    @Override
    public double getQ1() {
        return state.quantile(0.25);
    }

    @Override
    public double getQ2() {
        return state.quantile(0.5);
    }

    @Override
    public double getQ3() {
        return state.quantile(0.75);
    }

    @Override
    public String getMinAsString() {
        return valueAsString(Metrics.MIN.name());
    }

    @Override
    public String getMaxAsString() {
        return valueAsString(Metrics.MAX.name());
    }

    @Override
    public String getQ1AsString() {
        return valueAsString(Metrics.Q1.name());
    }

    @Override
    public String getQ2AsString() {
        return valueAsString(Metrics.Q2.name());
    }

    @Override
    public String getQ3AsString() {
        return valueAsString(Metrics.Q3.name());
    }

    @Override
    public double value(String name) {
        return Metrics.resolve(name).value(this);
    }

    @Override
    public Iterable<String> valueNames() {
        return METRIC_NAMES;
    }

    // for testing only
    DocValueFormat format() {
        return format;
    }

    // for testing only
    TDigestState state() {
        return state;
    }

    @Override
    public InternalBoxplot reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalBoxplot percentiles = (InternalBoxplot) aggregation;
            if (merged == null) {
                merged = new TDigestState(percentiles.state.compression());
            }
            merged.add(percentiles.state);
        }
        return new InternalBoxplot(name, merged, format, metadata);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        double[] whiskers = whiskers(state);
        builder.field("min", getMin());
        builder.field("max", getMax());
        builder.field("q1", getQ1());
        builder.field("q2", getQ2());
        builder.field("q3", getQ3());
        builder.field("lower", whiskers[0]);
        builder.field("upper", whiskers[1]);
        if (format != DocValueFormat.RAW) {
            builder.field("min_as_string", format.format(getMin()));
            builder.field("max_as_string", format.format(getMax()));
            builder.field("q1_as_string", format.format(getQ1()));
            builder.field("q2_as_string", format.format(getQ2()));
            builder.field("q3_as_string", format.format(getQ3()));
            builder.field("lower_as_string", format.format(whiskers[0]));
            builder.field("upper_as_string", format.format(whiskers[1]));
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), state);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalBoxplot that = (InternalBoxplot) obj;
        return Objects.equals(state, that.state);
    }
}
