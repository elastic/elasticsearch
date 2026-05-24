/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.AbstractNumericBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.DoublesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxLongsFromDocValuesBlockLoader.discardAllButLast;

/**
 * Loads the MAX {@code double} in each doc.
 */
public class MvMaxDoublesFromDocValuesBlockLoader extends DoublesBlockLoader {
    public MvMaxDoublesFromDocValuesBlockLoader(String fieldName, BlockDocValuesReader.ToDouble toDouble) {
        super(fieldName, toDouble);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new AbstractNumericBlockLoader.Sorted<>(this, "MvMaxDoublesFromDocValues", docValues) {
            @Override
            protected void readSortedDoc(int doc, BlockLoader.DoubleBuilder builder) throws IOException {
                if (values.docValues().advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                discardAllButLast(values.docValues());
                appendValue(builder, values.docValues().nextValue());
            }
        };
    }
}
