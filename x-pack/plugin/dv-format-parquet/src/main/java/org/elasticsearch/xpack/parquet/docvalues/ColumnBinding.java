/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.DocValuesType;
import org.elasticsearch.index.mapper.IdFieldMapper;

/**
 * Binds a Lucene doc values field to a column of the external Parquet file.
 *
 * @param fieldName the name of the Lucene field
 * @param fieldNumber the number of the field in the segment that holds the binding
 * @param parquetColumnPath the dotted path of the Parquet column
 * @param type the Lucene doc values type that the field is served as
 * @param conversion how the values of the column become the values that the mapped field type expects
 */
public record ColumnBinding(String fieldName, int fieldNumber, String parquetColumnPath, DocValuesType type, Conversion conversion) {

    public ColumnBinding(String fieldName, int fieldNumber, String parquetColumnPath, DocValuesType type) {
        this(fieldName, fieldNumber, parquetColumnPath, type, Conversion.NONE);
    }

    /** The marker path of a field that the producer synthesizes and that no Parquet column holds. */
    public static final String SYNTHESIZED = "";

    /**
     * The binding of the {@code _id} field of a columnar index. The {@code _id} of a document is the row number in the index, so no
     * file holds it, and the producer builds it from the position of the document in the pieces.
     */
    public static ColumnBinding synthesizedId(int fieldNumber) {
        return new ColumnBinding(IdFieldMapper.NAME, fieldNumber, SYNTHESIZED, DocValuesType.BINARY, Conversion.NONE);
    }

    public boolean isSynthesized() {
        return parquetColumnPath.equals(SYNTHESIZED);
    }
}
