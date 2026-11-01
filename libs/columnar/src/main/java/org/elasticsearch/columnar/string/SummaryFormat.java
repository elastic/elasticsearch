/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.columnar.substrate.ColumnInputs;
import org.elasticsearch.columnar.substrate.ColumnOutputs;

import java.io.IOException;
import java.util.List;

/**
 * What a column leaves behind for the merge that reads it next, and how it is laid out: the numbers alone,
 * counts beside a dictionary already holding the terms, or terms and counts of its own.
 */
final class SummaryFormat {

    private SummaryFormat() {}

    /**
     * @param dictionaryHoldsTheTerms whether the column's dictionary already holds every summarised term
     */
    static StringColumnMetadata.Summary write(
        Vocabulary.Terms vocabulary,
        boolean dictionaryHoldsTheTerms,
        long numValues,
        ChunkCodec chunkCodec,
        StringColumnOptions.Sizes sizes,
        ColumnOutputs outputs
    ) throws IOException {
        final IndexOutput data = outputs.data();
        final int size = vocabulary.summarySize();
        if (size == 0) {
            return new StringColumnMetadata.Summary(null, 0, 0, numValues, vocabulary.bestCoverage());
        }
        ValueStream.Metadata terms = null;
        if (dictionaryHoldsTheTerms == false) {
            final BytesRef term = new BytesRef();
            final ValueStream.Writer writer = new ValueStream.Writer(chunkCodec, sizes.escapeChunks(), sizes.valuesPerBlock(), outputs);
            for (int ordinal = 0; ordinal < size; ordinal++) {
                vocabulary.terms().get(vocabulary.summaryIds()[ordinal], term);
                writer.add(term);
            }
            terms = writer.finish();
        }
        final long countsOffset = data.getFilePointer();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            data.writeVLong(vocabulary.summaryCountOf(ordinal));
        }
        return new StringColumnMetadata.Summary(
            terms,
            countsOffset,
            data.getFilePointer() - countsOffset,
            numValues,
            vocabulary.bestCoverage()
        );
    }

    /** Reads {@code size} terms from {@code source} and the counts written beside them. */
    static void read(
        StringColumnMetadata.Summary summary,
        ColumnInputs inputs,
        ValueStream.Reader source,
        int size,
        List<BytesRef> terms,
        List<Long> counts
    ) throws IOException {
        final BytesRef term = new BytesRef();
        for (int ordinal = 0; ordinal < size; ordinal++) {
            source.get(ordinal, term);
            terms.add(BytesRef.deepCopyOf(term));
        }
        // Cloned rather than read in place: the caller's own reads are interleaved with these.
        final IndexInput in = inputs.data().clone();
        in.seek(summary.countsOffset());
        for (int ordinal = 0; ordinal < size; ordinal++) {
            counts.add(in.readVLong());
        }
    }
}
