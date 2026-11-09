/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.index.DocValuesType;
import org.elasticsearch.index.mapper.IdFieldMapper;

/**
 * Binds one Lucene doc values field to one column of the Parquet file.
 *
 * @param fieldName     the Lucene field name
 * @param fieldNumber   the Lucene field number, which must be the one of the segment's {@code FieldInfo}
 * @param parquetColumn the column name as the ES|QL Parquet reader names it, or {@link #SYNTHESIZED} for a field that no column holds
 * @param type          {@link DocValuesType#NUMERIC} or {@link DocValuesType#BINARY}
 * @param conversion    how the values of the column become the values that the mapped field type expects; only for numeric fields
 */
public record ColumnBinding(String fieldName, int fieldNumber, String parquetColumn, DocValuesType type, Conversion conversion) {

    /** The marker column of a field that the producer synthesizes and that no Parquet column holds. */
    public static final String SYNTHESIZED = "";

    public ColumnBinding {
        if (type != DocValuesType.NUMERIC && type != DocValuesType.BINARY) {
            throw new IllegalArgumentException("a Parquet column serves NUMERIC or BINARY doc values, not [" + type + "]");
        }
    }

    public ColumnBinding(String fieldName, int fieldNumber, String parquetColumn, DocValuesType type) {
        this(fieldName, fieldNumber, parquetColumn, type, Conversion.NONE);
    }

    /**
     * The binding of the {@code _id} field of a columnar index. The {@code _id} of a document is its row number in the index, so no
     * file holds it, and the producer builds it from the position of the document in the index.
     */
    public static ColumnBinding synthesizedId(int fieldNumber) {
        return new ColumnBinding(IdFieldMapper.NAME, fieldNumber, SYNTHESIZED, DocValuesType.BINARY, Conversion.NONE);
    }

    public boolean isSynthesized() {
        return parquetColumn.equals(SYNTHESIZED);
    }
}
