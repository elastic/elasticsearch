/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;

import java.io.IOException;

public abstract class ShapeValuesSource extends ValuesSource {
    public abstract ShapeValues shapeValues(LeafReaderContext context);

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        return FieldData.emptySortedBinary();
    }

    @Override
    public DocValueBits docsWithValue(LeafReaderContext context) {
        ShapeValues values = shapeValues(context);
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }
}
