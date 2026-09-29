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
import org.elasticsearch.columnar.ColumNARDocValuesFormat;

/**
 * A binary doc-values field that carries the {@link ColumNARDocValuesFormat#SINGLE_VALUED_ATTRIBUTE}
 * on its {@link FieldType}, so the attribute flows into the {@code FieldInfo} when the document is
 * first indexed. The ColumNAR producer reads it back to return the raw value bytes rather than
 * building a payload.
 */
public final class SingleValuedColumnarBinaryDocValuesField extends Field {

    public static final FieldType TYPE;
    static {
        TYPE = new FieldType();
        TYPE.setDocValuesType(DocValuesType.BINARY);
        TYPE.putAttribute(ColumNARDocValuesFormat.SINGLE_VALUED_ATTRIBUTE, "true");
        TYPE.freeze();
    }

    public SingleValuedColumnarBinaryDocValuesField(String name, BytesRef value) {
        super(name, TYPE);
        fieldsData = value;
    }
}
