/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Single-valued counterpart to Lucene's {@link FloatField}. Bundles a {@link FloatPoint}-compatible point with a
 * {@link NumericDocValuesField}-compatible doc value into a single indexable field.
 */
public final class SingleValuedFloatField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setDimensions(1, Float.BYTES);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.freeze();
    }

    public SingleValuedFloatField(String name, float value) {
        super(name, FIELD_TYPE);
        fieldsData = (long) NumericUtils.floatToSortableInt(value);
    }

    @Override
    public BytesRef binaryValue() {
        byte[] encodedPoint = new byte[Float.BYTES];
        FloatPoint.encodeDimension(NumericUtils.sortableIntToFloat(numericValue().intValue()), encodedPoint, 0);
        return new BytesRef(encodedPoint);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " <" + name + ':' + NumericUtils.sortableIntToFloat(numericValue().intValue()) + '>';
    }
}
