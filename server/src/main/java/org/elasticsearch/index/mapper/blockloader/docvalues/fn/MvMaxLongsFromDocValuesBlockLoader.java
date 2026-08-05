/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractNumericBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Loads the MAX {@code long} in each doc.
 */
public class MvMaxLongsFromDocValuesBlockLoader extends LongsBlockLoader {
    public MvMaxLongsFromDocValuesBlockLoader(String fieldName) {
        super(fieldName);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        // Own read loop so the per-document append compiles monomorphically rather than going megamorphic through a shared reader.
        return new AbstractNumericBlockLoader.Sorted("MvMaxLongsFromDocValues", docValues) {
            @Override
            public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                SortedNumericDocValues docValues = values.docValues();
                try (LongBuilder builder = factory.longsFromDocValues(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        if (docValues.advanceExact(docs.get(i)) == false) {
                            builder.appendNull();
                            continue;
                        }
                        discardAllButLast(docValues);
                        builder.appendLong(docValues.nextValue());
                    }
                    return builder.build();
                }
            }
        };
    }

    /**
     * Discard all doc values but the last ones in this position.
     */
    static void discardAllButLast(SortedNumericDocValues numericDocValues) throws IOException {
        int count = numericDocValues.docValueCount();
        for (int i = 0; i < count - 1; i++) {
            numericDocValues.nextValue();
        }
    }
}
