/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldSyntheticWriterHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper.Metric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that produces the last value of a field for downsampling.
 * Important note: This class assumes that field values are collected and sorted by descending order by time
 */
class LastValueFieldProducer extends AbstractDownsampleFieldProducer {
    private final boolean supportsMultiValue;
    Object lastValue = null;

    LastValueFieldProducer(String name, boolean producesMultiValue) {
        super(name);
        this.supportsMultiValue = producesMultiValue;
    }

    static LastValueFieldProducer createForLabel(String name) {
        return new LastValueFieldProducer(name, true);
    }

    static LastValueFieldProducer createForMetric(String name) {
        return new LastValueFieldProducer(name, false);
    }

    @Override
    public void reset() {
        isEmpty = true;
        lastValue = null;
    }

    /**
     * Collects the last value observed in these field values. This implementation assumes that field values are collected
     * and sorted by descending order by time. In this case, it assumes that the last value of the time is the first value
     * collected. Eventually, the implementation of this class ends up storing the first value it is empty and then
     * ignoring everything else.
     */
    @Override
    public void collect(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        if (isEmpty() == false) {
            return;
        }

        for (int i = 0; i < docIdBuffer.size(); i++) {
            int docId = docIdBuffer.get(i);
            if (docValues.advanceExact(docId) == false) {
                continue;
            }
            int docValuesCount = docValues.docValueCount();
            assert docValuesCount > 0;
            isEmpty = false;
            if (docValuesCount == 1 || supportsMultiValue == false) {
                lastValue = docValues.nextValue();
            } else {
                var values = new Object[docValuesCount];
                for (int j = 0; j < docValuesCount; j++) {
                    values[j] = docValues.nextValue();
                }
                lastValue = values;
            }
            // Only need to record one label value from one document, within in the same tsid-and-time-interval we only keep the first
            // with downsampling.
            return;
        }
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(name(), lastValue);
        }
    }

    public Object lastValue() {
        return lastValue;
    }

    public String sampleLabel() {
        return "last_value";
    }

    static final class AggregateMetricFieldProducer extends LastValueFieldProducer {

        private final Metric metric;

        AggregateMetricFieldProducer(String name, Metric metric) {
            super(name, true);
            this.metric = metric;
        }

        @Override
        public String sampleLabel() {
            return metric.name();
        }
    }

    static final class HistogramLastLastValueFieldProducer extends LastValueFieldProducer {
        HistogramLastLastValueFieldProducer(String name) {
            super(name, true);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                final HistogramValue histogramValue = (HistogramValue) lastValue();
                final List<Double> values = new ArrayList<>();
                final List<Long> counts = new ArrayList<>();
                while (histogramValue.next()) {
                    values.add(histogramValue.value());
                    counts.add(histogramValue.count());
                }
                builder.startObject(name()).field("counts", counts).field("values", values).endObject();
            }
        }
    }

    static final class FlattenedLastValueFieldProducer extends LastValueFieldProducer {

        FlattenedLastValueFieldProducer(String name) {
            super(name, true);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());

                var value = lastValue();
                List<BytesRef> list;
                if (value instanceof Object[] values) {
                    list = new ArrayList<>(values.length);
                    for (Object v : values) {
                        list.add(new BytesRef(v.toString()));
                    }
                } else {
                    list = List.of(new BytesRef(value.toString()));
                }

                var iterator = list.iterator();
                var helper = new FlattenedFieldSyntheticWriterHelper(() -> {
                    if (iterator.hasNext()) {
                        return iterator.next();
                    } else {
                        return null;
                    }
                });
                helper.write(builder);
                builder.endObject();
            }
        }
    }
}
