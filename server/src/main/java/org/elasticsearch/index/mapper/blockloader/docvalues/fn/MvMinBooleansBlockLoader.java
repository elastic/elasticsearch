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
import org.elasticsearch.index.mapper.blockloader.docvalues.BooleansBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingSortedNumericDocValues;

import java.io.IOException;

/**
 * Loads the MIN {@code boolean} in each doc. Think of it like {@code ALL}.
 */
public class MvMinBooleansBlockLoader extends BooleansBlockLoader {
    public MvMinBooleansBlockLoader(String fieldName) {
        super(fieldName);
    }

    @Override
    protected ColumnAtATimeReader sortedReader(TrackingSortedNumericDocValues docValues) {
        return new AbstractNumericBlockLoader.Sorted<>(this, "MvMinBooleansFromDocValues", docValues) {
            @Override
            protected void readSortedDoc(int doc, BlockLoader.BooleanBuilder builder) throws IOException {
                if (values.docValues().advanceExact(doc) == false) {
                    builder.appendNull();
                    return;
                }
                appendValue(builder, values.docValues().nextValue());
            }
        };
    }
}
