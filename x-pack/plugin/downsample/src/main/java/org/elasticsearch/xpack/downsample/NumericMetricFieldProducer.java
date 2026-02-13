/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class that collects all raw values for a numeric metric field and computes its aggregate (downsampled)
 * values. Based on the supported metric types, the subclasses of this class compute values for
 * gauge and metric types.
 */
abstract sealed class NumericMetricFieldProducer extends AbstractDownsampleFieldProducer<SortedNumericDoubleValues> {

    NumericMetricFieldProducer(String name) {
        super(name);
    }

    static final double MAX_NO_VALUE = -Double.MAX_VALUE;
    static final double MIN_NO_VALUE = Double.MAX_VALUE;

    /**
     * {@link NumericMetricFieldProducer} implementation for creating an aggregate gauge metric field
     */
    static final class AggregateGauge extends NumericMetricFieldProducer {

        double max = MAX_NO_VALUE;
        double min = MIN_NO_VALUE;
        final CompensatedSum sum = new CompensatedSum();
        long count;

        AggregateGauge(String name) {
            super(name);
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId) == false) {
                    continue;
                }
                isEmpty = false;
                int docValuesCount = docValues.docValueCount();
                for (int j = 0; j < docValuesCount; j++) {
                    double value = docValues.nextValue();
                    this.max = Math.max(value, max);
                    this.min = Math.min(value, min);
                    sum.add(value);
                    count++;
                }
            }
        }

        @Override
        public void reset() {
            isEmpty = true;
            max = MAX_NO_VALUE;
            min = MIN_NO_VALUE;
            sum.reset(0, 0);
            count = 0;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());
                builder.field("min", min);
                builder.field("max", max);
                builder.field("sum", sum.value());
                builder.field("value_count", count);
                builder.endObject();
            }
        }
    }

    /**
     * {@link NumericMetricFieldProducer} implementation for sampling the last value of a numeric metric field.
     * Important note: This class assumes that field values are collected and sorted by descending order by time.
     */
    static final class LastValue extends NumericMetricFieldProducer {

        double lastValue = Double.NaN;

        LastValue(String name) {
            super(name);
        }

        @Override
        public void collect(SortedNumericDoubleValues docValues, IntArrayList docIdBuffer) throws IOException {
            if (isEmpty() == false) {
                return;
            }

            for (int i = 0; i < docIdBuffer.size(); i++) {
                int docId = docIdBuffer.get(i);
                if (docValues.advanceExact(docId)) {
                    isEmpty = false;
                    lastValue = docValues.nextValue();
                    return;
                }
            }
        }

        public Double lastValue() {
            if (isEmpty()) {
                return null;
            }
            return lastValue;
        }

        @Override
        public void reset() {
            isEmpty = true;
            lastValue = Double.NaN;
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.field(name(), lastValue);
            }
        }
    }
}
