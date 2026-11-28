/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.columnar.numeric.NumericColumnSource;

import java.io.IOException;

/**
 * Finds the doc values skipper of a field, including a ColumNAR numeric column's.
 *
 * <p>Lucene gives binary doc values no skip index, since their bytes have no order it knows. A ColumNAR numeric column is stored
 * as binary doc values but holds numbers, and keeps a skip index over them that it hands out as a {@link NumericColumnSource}.
 * It is found through the reader's doc values, so readers that wrap the column, such as field level security, apply.
 */
public final class DocValuesSkippers {
    private DocValuesSkippers() {}

    /** The skipper of {@code field} in {@code reader}, or {@code null} if it has none. */
    public static DocValuesSkipper get(LeafReader reader, String field) throws IOException {
        DocValuesSkipper skipper = reader.getDocValuesSkipper(field);
        if (skipper != null) {
            return skipper;
        }
        FieldInfo info = reader.getFieldInfos().fieldInfo(field);
        if (info == null || info.getDocValuesType() != DocValuesType.BINARY) {
            return null;
        }
        BinaryDocValues values = reader.getBinaryDocValues(field);
        return values instanceof NumericColumnSource column ? column.skipper() : null;
    }
}
