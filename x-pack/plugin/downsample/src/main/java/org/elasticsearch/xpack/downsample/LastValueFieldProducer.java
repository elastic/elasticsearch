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
    // When downsampling metrics, we only keep one value even if the field was a multi-value field.
    // For labels, we preserve all the values of the last occurrence.
    private final boolean supportsMultiValue;
    Object lastValue = null;

    LastValueFieldProducer(String name, boolean producesMultiValue) {
        super(name);
        this.supportsMultiValue = producesMultiValue;
    }

    /**
     * Creates a producer that can be used for downsampling labels. It works for all types apart from
     * `aggregate_metric_double`, if the field type is aggregate metric double, please use
     * {@link LastValueFieldProducer#createForAggregateSubMetricLabel(String, Metric)}.
     */
    static LastValueFieldProducer createForLabel(String name, String fieldType) {
        assert "aggregate_metric_double".equals(fieldType) == false
            : "field type cannot be aggregate metric double: " + fieldType + " for field " + name;
        if ("histogram".equals(fieldType)) {
            return new LastValueFieldProducer.HistogramFieldProducer(name, true);
        } else if ("flattened".equals(fieldType)) {
            return new LastValueFieldProducer.FlattenedFieldProducer(name, true);
        }
        return new LastValueFieldProducer(name, true);
    }

    /**
     * Creates a producer that can be used for downsampling labels. It works for all types apart from
     * `aggregate_metric_double`, if the field type is aggregate metric double, please use
     * {@link LastValueFieldProducer#createForAggregateSubMetricMetric(String, Metric)} (String, Metric).}
     */
    static LastValueFieldProducer createForMetric(String name) {
        return new LastValueFieldProducer(name, false);
    }

    /**
     * Creates a producer that can be used for downsampling ONLY a sub-metric of an aggregate metric double labels. For
     * other types of labels please use {@link LastValueFieldProducer#createForLabel(String, String)}.
     */
    static AggregateSubMetricFieldProducer createForAggregateSubMetricLabel(String name, Metric metric) {
        return new AggregateSubMetricFieldProducer(name, metric, true);
    }

    /**
     * Creates a producer that can be used for downsampling ONLY a sub-metric of an aggregate metric double metrics. For
     * other types of metrics please use {@link LastValueFieldProducer#createForMetric(String)}.
     */
    static AggregateSubMetricFieldProducer createForAggregateSubMetricMetric(String name, Metric metric) {
        return new AggregateSubMetricFieldProducer(name, metric, false);
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

    /**
     * This producer is used to downsample by keeping the last value the sub-metric of an aggregate metric double.
     */
    static final class AggregateSubMetricFieldProducer extends LastValueFieldProducer {

        private final Metric metric;

        private AggregateSubMetricFieldProducer(String name, Metric metric, boolean supportsMultiValue) {
            super(name, supportsMultiValue);
            this.metric = metric;
        }

        public String subMetric() {
            return metric.name();
        }
    }

    static final class HistogramFieldProducer extends LastValueFieldProducer {
        private HistogramFieldProducer(String name, boolean producesMultiValue) {
            super(name, producesMultiValue);
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

    static final class FlattenedFieldProducer extends LastValueFieldProducer {

        private FlattenedFieldProducer(String name, boolean producesMultiValue) {
            super(name, producesMultiValue);
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
