/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

public final class MultiValuedBinaryDVLeafFieldData implements LeafFieldData {
    private final String fieldName;
    private final LeafReader leafReader;
    private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

    MultiValuedBinaryDVLeafFieldData(
        String fieldName,
        LeafReader leafReader,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
    ) {
        super();
        this.fieldName = fieldName;
        this.leafReader = leafReader;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        try {
            // Need to return a new instance each time this gets invoked,
            // otherwise a positioned or exhausted instance can be returned:
            BinaryDocValues binaryValues = DocValues.getBinary(leafReader, fieldName);
            return new MultiValuedSortedBinaryDocValues(binaryValues);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        return toScriptFieldFactory.getScriptFieldFactory(getBytesValues(), name);
    }

}
