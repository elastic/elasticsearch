/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;

final class MultiVectorDVLeafFieldData implements LeafFieldData {
    private final LeafReader reader;
    private final String field;
    private final IndexVersion indexVersion;
    private final DenseVectorFieldMapper.ElementType elementType;
    private final int dims;

    MultiVectorDVLeafFieldData(
        LeafReader reader,
        String field,
        IndexVersion indexVersion,
        DenseVectorFieldMapper.ElementType elementType,
        int dims
    ) {
        this.reader = reader;
        this.field = field;
        this.indexVersion = indexVersion;
        this.elementType = elementType;
        this.dims = dims;
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        // TODO
        return null;
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("String representation of doc values for multi-vector fields is not supported");
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}
