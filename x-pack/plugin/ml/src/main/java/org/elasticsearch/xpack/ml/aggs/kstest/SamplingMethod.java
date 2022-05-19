/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class SamplingMethod implements Writeable {

    public static SamplingMethod fromString(String value) {
        return switch (value) {
            case UpperTail.NAME -> new UpperTail();
            case LowerTail.NAME -> new LowerTail();
            case Uniform.NAME -> new Uniform();
            default -> throw new IllegalArgumentException("Unrecognized sampling_method [" + value + "]");
        };
    }

    public static SamplingMethod fromStream(StreamInput in) throws IOException {
        String name = in.readString();
        return fromString(name);
    }

    public abstract String getName();

    protected abstract double[] cdfPoints();

    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(getName());
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    /**
     * A sampling methodology that provides CDF points that tend towards the upper values.
     * Meaning, more values are sampled from the upper range
     */
    public static class UpperTail extends SamplingMethod {
        public static final String NAME = "upper_tail";
        private static final double[] CDF_POINTS;

        static {
            List<Double> pts = new ArrayList<>();
            for (int i = 1; i < 11; i++) {
                for (int j = 0; j < i; j++) {
                    pts.add(0.1 * (i - 1) + 0.05 / i + 0.1 * j / i);
                }
            }
            CDF_POINTS = pts.stream().mapToDouble(Double::doubleValue).toArray();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        protected double[] cdfPoints() {
            return CDF_POINTS;
        }
    }

    /**
     * A sampling methodology that provides CDF points that tend towards the lower values.
     * Meaning, more values are sampled from the lower range
     */
    public static class LowerTail extends SamplingMethod {
        public static final String NAME = "lower_tail";
        private static final double[] CDF_POINTS;

        static {
            CDF_POINTS = new double[UpperTail.CDF_POINTS.length];
            for (int i = 0; i < CDF_POINTS.length; i++) {
                CDF_POINTS[i] = 1 - UpperTail.CDF_POINTS[i];
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        protected double[] cdfPoints() {
            return CDF_POINTS;
        }
    }

    /**
     * A sampling methodology that provides CDF points that is uniformly sampled.
     * Meaning, all values are sampled equally
     */
    public static class Uniform extends SamplingMethod {
        public static final String NAME = "uniform";
        private static final double[] CDF_POINTS;

        static {
            CDF_POINTS = new double[UpperTail.CDF_POINTS.length];
            final double interval = 1.0 / (UpperTail.CDF_POINTS.length + 1);
            CDF_POINTS[0] = interval;
            for (int i = 1; i < CDF_POINTS.length; i++) {
                CDF_POINTS[i] = CDF_POINTS[i - 1] + interval;
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        protected double[] cdfPoints() {
            return CDF_POINTS;
        }
    }

}
