/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSource;

import java.io.IOException;

final class CartesianPointDVLeafFieldData extends LeafCartesianPointFieldData {
    private final LeafReader reader;
    private final String fieldName;

    CartesianPointDVLeafFieldData(
        LeafReader reader,
        String fieldName,
        ToScriptFieldFactory<CartesianPointValuesSource.MultiCartesianPointValues> toScriptFieldFactory
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
    public SortedNumericDocValues getSortedNumericDocValues() {
        try {
            return DocValues.getSortedNumeric(reader, fieldName);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
