/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.fn;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.Warnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.BreakerPageBudget;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.Warnings.registerSingleValueWarning;

/**
 * Reads the byte lengths of a page of a string column's documents from the lengths the column stores, without reading
 * a value. Shared by the readers of a columnar keyword's two framings, which sit on the same column.
 */
final class ColumnarByteLengthPageReader implements Releasable {

    private final Warnings warnings;
    private int[] wanted = new int[0];
    private int[] counts = new int[0];
    private int[] lengths = new int[0];
    /**
     * Charged before the column grows the page storage it resolves this reader's documents in, and released with
     * this reader, since that storage lives as long as the reader does.
     */
    private final BreakerPageBudget budget;

    ColumnarByteLengthPageReader(Warnings warnings, CircuitBreaker breaker) {
        this.warnings = warnings;
        this.budget = new BreakerPageBudget(breaker);
    }

    /**
     * The lengths of {@code docs} from {@code offset} on. A document holding one non-null value answers its length,
     * one holding none answers null, and one holding several answers null with a single-value warning.
     */
    BlockLoader.Block read(StringColumnSource columnar, BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset)
        throws IOException {
        final int count = docs.count() - offset;
        if (wanted.length < count) {
            wanted = new int[ArrayUtil.oversize(count, Integer.BYTES)];
            counts = new int[wanted.length];
            lengths = new int[wanted.length];
        }
        for (int i = 0; i < count; i++) {
            wanted[i] = docs.get(offset + i);
        }
        columnar.reader().readByteLengths(wanted, 0, count, counts, lengths, budget);
        try (BlockLoader.IntBuilder builder = factory.ints(count)) {
            for (int i = 0; i < count; i++) {
                if (counts[i] == 1) {
                    builder.appendInt(lengths[i]);
                } else {
                    if (counts[i] > 1) {
                        registerSingleValueWarning(warnings);
                    }
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public void close() {
        budget.close();
    }
}
