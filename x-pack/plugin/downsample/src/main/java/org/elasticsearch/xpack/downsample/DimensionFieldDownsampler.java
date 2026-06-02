/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The dimension field downsampler reads the value of the last seen document per tsid, even if it's missing. Considering that dimensions
 * are the same for the same tsid, it is guaranteed that all documents with a tsid will have the same value. Consequently, we do not reset
 * it every time we start a new bucket but only if the tsid changes.
 */
public final class DimensionFieldDownsampler extends AbstractFieldDownsampler<FormattedDocValues> {

    private final MappedFieldType fieldType;
    private Object dimensionValue = null;

    DimensionFieldDownsampler(final String name, final MappedFieldType fieldType, final IndexFieldData<?> fieldData) {
        super(name, fieldData);
        this.fieldType = fieldType;
    }

    @Override
    public void reset() {
        // We do not reset dimensions unless tsid is reset.
    }

    public void tsidReset() {
        isEmpty = true;
        dimensionValue = null;
    }

    /**
     * Use {@link #collectOnce(FormattedDocValues, IntArrayList)} instead.
     * throws UnsupportedOperationException
     */
    @Override
    public void collect(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        throw new UnsupportedOperationException("This producer should be collected using the collectOnce method.");
    }

    public void collectOnce(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        // We only ensure we collect once with an assertion because we do it for performance reasons,
        // and it should be detected during development.
        assert isEmpty() : "dimension downsamplers should only be called once per tsid";

        // Only need to record one dimension value from one document, within in the same tsid-and-time-interval bucket values are the same.
        if (docIdBuffer.isEmpty() == false) {
            int docId = docIdBuffer.get(0);
            if (docValues.advanceExact(docId)) {
                int docValueCount = docValues.docValueCount();
                assert docValueCount > 0;
                var value = retrieveDimensionValues(docValues);
                Objects.requireNonNull(value);
                this.dimensionValue = value;
                this.isEmpty = false;
            }
        }
    }

    @Override
    public FormattedDocValues getLeaf(LeafReaderContext context) {
        DocValueFormat format = fieldType.docValueFormat(null, null);
        return fieldData.load(context).getFormattedValues(format);
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(name(), dimensionValue);
        }
    }

    public Object dimensionValue() {
        return dimensionValue;
    }

    private Object retrieveDimensionValues(FormattedDocValues docValues) throws IOException {
        int docValueCount = docValues.docValueCount();
        assert docValueCount > 0;
        Object value;
        if (docValueCount == 1) {
            value = docValues.nextValue();
        } else {
            var values = new Object[docValueCount];
            for (int j = 0; j < docValueCount; j++) {
                values[j] = docValues.nextValue();
            }
            value = values;
        }
        return value;
    }

    /**
     * Retrieve field value fetchers for a list of dimensions.
     */
    static List<DimensionFieldDownsampler> create(
        final SearchExecutionContext context,
        final String[] dimensions,
        final Map<String, String> multiFieldSources,
        DownsamplerCountPerValueType fieldCounts
    ) {
        List<DimensionFieldDownsampler> downsamplers = new ArrayList<>();
        for (String dimension : dimensions) {
            String sourceFieldName = multiFieldSources.getOrDefault(dimension, dimension);
            MappedFieldType fieldType = context.getFieldType(sourceFieldName);
            assert fieldType != null : "Unknown type for dimension field: [" + sourceFieldName + "]";

            if (context.fieldExistsInIndex(fieldType.name())) {
                fieldCounts.increaseDimensionFields();
                final IndexFieldData<?> fieldData = context.getForField(fieldType, MappedFieldType.FielddataOperation.SEARCH);
                if (fieldType instanceof FlattenedFieldMapper.KeyedFlattenedFieldType flattenedFieldType) {
                    // Name of the field type and name of the dimension are different in this case.
                    var dimensionName = flattenedFieldType.rootName() + '.' + flattenedFieldType.key();
                    downsamplers.add(new DimensionFieldDownsampler(dimensionName, fieldType, fieldData));
                } else {
                    downsamplers.add(new DimensionFieldDownsampler(dimension, fieldType, fieldData));
                }
            }
        }
        return Collections.unmodifiableList(downsamplers);
    }
}
