/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldSyntheticWriterHelper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that downsamples a label field for downsampling by keeping the last value.
 * Important note: This class assumes that field values are collected and sorted by descending order by time
 */
class LastValueFieldDownsampler extends AbstractFieldDownsampler<FormattedDocValues> {

    private final MappedFieldType fieldType;
    Object lastValue = null;

    LastValueFieldDownsampler(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        super(name, fieldData);
        this.fieldType = fieldType;
    }

    /**
     * Creates a producer that can be used for downsampling labels.
     */
    static LastValueFieldDownsampler create(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        assert AggregateMetricDoubleFieldDownsampler.supportsFieldType(fieldType) == false
            && ExponentialHistogramFieldDownsampler.supportsFieldType(fieldType) == false
            && TDigestHistogramFieldDownsampler.supportsFieldType(fieldType) == false
            : "field '" + name + "' of type '" + fieldType.typeName() + "' should be processed by a dedicated downsampler";
        if ("flattened".equals(fieldType.typeName())) {
            return new LastValueFieldDownsampler.FlattenedFieldProducer(name, fieldType, fieldData);
        }
        return new LastValueFieldDownsampler(name, fieldType, fieldData);
    }

    @Override
    public void reset() {
        isEmpty = true;
        lastValue = null;
    }

    @Override
    public FormattedDocValues getLeaf(LeafReaderContext context) {
        DocValueFormat format = fieldType.docValueFormat(null, null);
        return fieldData.load(context).getFormattedValues(format);
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

    static final class FlattenedFieldProducer extends LastValueFieldDownsampler {

        private FlattenedFieldProducer(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
            super(name, fieldType, fieldData);
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
