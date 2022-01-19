/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer.metrics;

public abstract class MetricCollector {
    public final String name;

    protected MetricCollector(String name) {
        this.name = name;
    }

    public abstract void collect(double number);

    public abstract Number get();

    public abstract void reset();

    public static class Max extends MetricCollector {
        private Double max;

        public Max() {
            super("max");
        }

        @Override
        public void collect(double value) {
            this.max = max != null ? Math.max(value, max) : value;
        }

        @Override
        public Number get() {
            return max;
        }

        @Override
        public void reset() {
            max = null;
        }
    }

    public static class Min extends MetricCollector {
        private Double min;

        public Min() {
            super("min");
        }

        @Override
        public void collect(double value) {
            this.min = min != null ? Math.min(value, min) : value;
        }

        @Override
        public Number get() {
            return min;
        }

        @Override
        public void reset() {
            min = null;
        }
    }

    public static class Sum extends MetricCollector {
        private double sum = 0;

        public Sum() {
            super("sum");
        }

        @Override
        public void collect(double value) {
            // TODO: switch to Kahan summation ?
            this.sum += value;
        }

        @Override
        public Number get() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }

    public static class ValueCount extends MetricCollector {
        private long count;

        public ValueCount() {
            super("value_count");
        }

        @Override
        public void collect(double value) {
            count++;
        }

        @Override
        public Number get() {
            return count;
        }

        @Override
        public void reset() {
            count = 0;
        }
    }
}
