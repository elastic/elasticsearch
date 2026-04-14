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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Single-valued variant of Lucene's {@link org.apache.lucene.document.LongField}. Uses {@link DocValuesType#NUMERIC} instead of
 * {@link DocValuesType#SORTED_NUMERIC}, enforcing single-valuedness natively and enabling storage optimizations.
 */
final class SingleValuedLongField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setDimensions(1, Long.BYTES);
        FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        FIELD_TYPE.freeze();
    }

    SingleValuedLongField(String name, long value) {
        super(name, FIELD_TYPE);
        fieldsData = value;
    }

    @Override
    public BytesRef binaryValue() {
        byte[] bytes = new byte[Long.BYTES];
        NumericUtils.longToSortableBytes((Long) fieldsData, bytes, 0);
        return new BytesRef(bytes);
    }
}
