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
import org.elasticsearch.index.mapper.blockloader.docvalues.BooleansBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxLongsFromDocValuesBlockLoader.discardAllButLast;

/**
 * Loads the MAX {@code boolean} in each doc. Think of like {@code ANY}.
 */
public class MvMaxBooleansBlockLoader extends BooleansBlockLoader {
    public MvMaxBooleansBlockLoader(String fieldName) {
        super(fieldName);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        // Own read loop so the per-document append compiles monomorphically rather than going megamorphic through a shared reader.
        return new AbstractNumericBlockLoader.Sorted("MvMaxBooleansFromDocValues", docValues) {
            @Override
            public Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
                SortedNumericDocValues docValues = values.docValues();
                try (BooleanBuilder builder = factory.booleansFromDocValues(docs.count() - offset)) {
                    for (int i = offset; i < docs.count(); i++) {
                        if (docValues.advanceExact(docs.get(i)) == false) {
                            builder.appendNull();
                            continue;
                        }
                        discardAllButLast(docValues);
                        builder.appendBoolean(docValues.nextValue() != 0);
                    }
                    return builder.build();
                }
            }
        };
    }
}
