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
import org.elasticsearch.index.mapper.flattened.FlattenedFieldSyntheticWriterHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.core.analytics.mapper.TDigestFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that produces the last value of a label field for downsampling.
 * Important note: This class assumes that field values are collected and sorted by descending order by time
 */
class LastValueFieldProducer extends AbstractDownsampleFieldProducer<FormattedDocValues> {
    Object lastValue = null;

    LastValueFieldProducer(String name) {
        super(name);
    }

    /**
     * Creates a producer that can be used for downsampling labels.
     */
    static LastValueFieldProducer create(String name, String fieldType) {
        assert AggregateMetricDoubleFieldMapper.CONTENT_TYPE.equals(fieldType) == false
            : "field type cannot be aggregate metric double: " + fieldType + " for field " + name;
        assert ExponentialHistogramFieldProducer.TYPE.equals(fieldType) == false
            : "field type cannot be exponential histogram: " + fieldType + " for field " + name;
        assert HistogramFieldMapper.CONTENT_TYPE.equals(fieldType) == false
            : "field type cannot be histogram: " + fieldType + " for field " + name;
        assert TDigestFieldMapper.CONTENT_TYPE.equals(fieldType) == false
            : "field type cannot be histogram: " + fieldType + " for field " + name;
        if ("flattened".equals(fieldType)) {
            return new LastValueFieldProducer.FlattenedFieldProducer(name);
        }
        return new LastValueFieldProducer(name);
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
            if (docValuesCount == 1) {
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

    static final class FlattenedFieldProducer extends LastValueFieldProducer {

        private FlattenedFieldProducer(String name) {
            super(name);
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
