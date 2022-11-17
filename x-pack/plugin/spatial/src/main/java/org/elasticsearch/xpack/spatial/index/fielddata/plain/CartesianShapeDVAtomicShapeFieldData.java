/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafShapeFieldData;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;

import java.io.IOException;

final class CartesianShapeDVAtomicShapeFieldData extends LeafShapeFieldData<CartesianShapeValues> {
    private final LeafReader reader;
    private final String fieldName;

    CartesianShapeDVAtomicShapeFieldData(
        LeafReader reader,
        String fieldName,
        ToScriptFieldFactory<CartesianShapeValues> toScriptFieldFactory
    ) {
        super(toScriptFieldFactory);
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public CartesianShapeValues getShapeValues() {
        try {
            final BinaryDocValues binaryValues = DocValues.getBinary(reader, fieldName);
            final CartesianShapeValues.CartesianShapeValue shapeValue = new CartesianShapeValues.CartesianShapeValue();
            return new CartesianShapeValues() {

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return binaryValues.advanceExact(doc);
                }

                @Override
                public ValuesSourceType valuesSourceType() {
                    return CartesianShapeValuesSourceType.instance();
                }

                @Override
                public CartesianShapeValue value() throws IOException {
                    shapeValue.reset(binaryValues.binaryValue());
                    return shapeValue;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
