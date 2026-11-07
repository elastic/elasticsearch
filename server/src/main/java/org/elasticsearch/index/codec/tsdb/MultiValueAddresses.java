/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.es95.ES95DefaultSortedSetCodec;

import java.io.IOException;

/**
 * The per-doc addresses table of a multi-valued stream in the blocked layout: a monotonic column with one
 * entry per document with a value, plus a final fencepost, giving the start of each document's slice in the
 * flattened value stream. It follows the stream's own metadata and data, and exists only when the field has
 * more values than documents.
 *
 * <p>Shared by the sorted-numeric path of {@link AbstractTSDBDocValuesConsumer} and by the ordinal codecs
 * that keep the blocked layout for multi-valued {@code SortedSet} fields ({@link ES95DefaultSortedSetCodec}).
 * A codec that lays the stream out so each document's slice is self-delimiting simply does not call this
 * class, and the shared consumer and producer are none the wiser.
 */
public final class MultiValueAddresses {

    private MultiValueAddresses() {}

    /**
     * Writes one field's value stream and returns its {@link DocValueFieldCountStats}.
     *
     * <p>During optimized merges, re-iterating the merge-sorted values to collect the per-doc value counts is
     * expensive, so an {@link OffsetsAccumulator} is passed to collect them in the same pass. {@link #write}
     * calls this with a non-null accumulator during optimized merges when the field is multi-valued, and with
     * {@code null} otherwise.
     */
    @FunctionalInterface
    public interface StreamWriter {
        DocValueFieldCountStats write(@Nullable OffsetsAccumulator accumulator) throws IOException;
    }

    /**
     * Writes the value stream through {@code streamWriter} and then the addresses table, when the field has
     * more values than documents.
     *
     * <p>With merge statistics available, the counts are known up front and the table is accumulated in the
     * same pass as the stream; otherwise the stream is written first and the values are iterated a second
     * time to build the table.
     */
    public static DocValueFieldCountStats write(
        final NumericWriteContext ctx,
        final FieldInfo field,
        final TsdbDocValuesProducer valuesSource,
        final StreamWriter streamWriter
    ) throws IOException {
        final IndexOutput meta = ctx.meta();
        final IndexOutput data = ctx.data();
        final int directMonotonicBlockShift = ctx.formatConfig().directMonotonicBlockShift();
        if (valuesSource.mergeStats.supported()) {
            int numDocsWithField = valuesSource.mergeStats.sumNumDocsWithField();
            long numValues = valuesSource.mergeStats.sumNumValues();
            if (numDocsWithField == numValues) {
                return streamWriter.write(null);
            }
            assert numValues > numDocsWithField;
            try (
                OffsetsAccumulator accumulator = new OffsetsAccumulator(
                    ctx.dir(),
                    ctx.ioContext(),
                    data,
                    numDocsWithField,
                    directMonotonicBlockShift
                )
            ) {
                final DocValueFieldCountStats stats = streamWriter.write(accumulator);
                accumulator.build(meta, data);
                return stats;
            }
        }

        final DocValueFieldCountStats stats = streamWriter.write(null);
        assert stats.numValues() >= stats.numDocsWithField();
        if (stats.numValues() > stats.numDocsWithField()) {
            long start = data.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(directMonotonicBlockShift);

            final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(
                meta,
                data,
                stats.numDocsWithField() + 1L,
                directMonotonicBlockShift
            );
            long addr = 0;
            addressesWriter.add(addr);
            SortedNumericDocValues values = valuesSource.getSortedNumeric(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                addr += values.docValueCount();
                addressesWriter.add(addr);
            }
            addressesWriter.finish();
            meta.writeLong(data.getFilePointer() - start);
        }
        return stats;
    }

    /**
     * Reads the addresses table metadata written by {@link #write} into {@code entry}, whose value and
     * document counts must already be populated from the stream's own metadata. Reads nothing when the field
     * has as many values as documents, matching what {@link #write} emitted.
     */
    public static void readMeta(final IndexInput meta, final SortedNumericEntry entry) throws IOException {
        if (entry.numDocsWithField != entry.numValues) {
            entry.addressesOffset = meta.readLong();
            final int blockShift = meta.readVInt();
            entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
            entry.addressesLength = meta.readLong();
        }
    }
}
