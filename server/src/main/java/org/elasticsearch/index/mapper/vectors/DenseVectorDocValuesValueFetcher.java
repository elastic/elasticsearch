/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ValueFetcher} for {@code dense_vector} fields that reads the vector from doc values rather than
 * from {@code _source}. Used with synthetic source, where {@code _source} would have to be rebuilt from the
 * same doc values anyway.
 */
class DenseVectorDocValuesValueFetcher implements ValueFetcher {
    private final VectorIndexFieldData fieldData;
    private final DocValueFormat format;

    private FormattedDocValues values;

    DenseVectorDocValuesValueFetcher(VectorIndexFieldData fieldData, DocValueFormat format) {
        this.fieldData = fieldData;
        this.format = format;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        values = fieldData.load(context).getFormattedValues(format);
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (values.advanceExact(doc) == false) {
            return List.of();
        }
        return unpack(values.nextValue());
    }

    /**
     * Flattens the single value {@link FormattedDocValues} reports per document into one entry per dimension.
     */
    private static List<Object> unpack(Object value) {
        switch (value) {
            case String base64 -> {
                return List.of(base64);
            }
            case float[] floats -> {
                List<Object> dimensions = new ArrayList<>(floats.length);
                for (float v : floats) {
                    dimensions.add(v);
                }
                return dimensions;
            }
            // Byte and bit dimensions are widened to Float to match what DenseVectorSourceValueFetcher returns
            case Byte[] bytes -> {
                List<Object> dimensions = new ArrayList<>(bytes.length);
                for (Byte v : bytes) {
                    dimensions.add(v.floatValue());
                }
                return dimensions;
            }
            default -> throw new IllegalStateException("unexpected dense vector doc value [" + value.getClass().getSimpleName() + "]");
        }
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }
}
