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
import org.elasticsearch.index.mapper.BlockLoader;
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
        return new AbstractNumericBlockLoader.Sorted<>(this, "MvMaxLongsFromDocValues", docValues) {
            @Override
            protected void readSortedDoc(int doc, BlockLoader.LongBuilder builder) throws IOException {
                if (values.docValues().advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                discardAllButLast(values.docValues());
                appendValue(builder, values.docValues().nextValue());
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
