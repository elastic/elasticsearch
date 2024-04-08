/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.io.IOException;

/**
 * An {@link LeafFieldData} implementation that uses Lucene {@link SortedSetDocValues}.
 */
public final class SortedSetBytesLeafFieldData extends AbstractLeafOrdinalsFieldData {

    private final LeafReader reader;
    private final String field;

    SortedSetBytesLeafFieldData(LeafReader reader, String field, ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory) {
        super(toScriptFieldFactory);
        this.reader = reader;
        this.field = field;
    }

    @Override
    public SortedSetDocValues getOrdinalsValues() {
        try {
            return DocValues.getSortedSet(reader, field);
        } catch (IOException e) {
            throw new IllegalStateException("cannot load docvalues", e);
        }
    }

    @Override
    public void close() {}

    @Override
    public long ramBytesUsed() {
        return 0; // unknown
    }

}
