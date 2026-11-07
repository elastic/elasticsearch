/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalReadContext;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSetOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedSetRunTableLayout;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;

import java.io.IOException;

/**
 * {@link SortedSetOrdinalCodec} that writes multi-valued dimension fields in the run-table layout and
 * delegates every other field to a {@code fallback} codec. It is the multi-valued counterpart of
 * {@link RunTableSortedCodec} and uses the same {@link RunTableGate} and discriminator byte.
 *
 * <p>The {@code _tsid} of a series covers every value of a multi-valued dimension, so within a series the
 * set of ordinals is constant, and TSDB segments are sorted by series. The run-table layout stores each run's
 * set once, instead of one set per doc plus the per-doc addresses table of the blocked layout, so its size
 * grows with the number of series rather than the number of docs. Docs without a value form runs of the
 * empty set.
 */
final class RunTableSortedSetCodec implements SortedSetOrdinalCodec {

    private final SortedSetOrdinalCodec fallback;
    @Nullable
    private final FieldContextResolver fieldContextResolver;

    /**
     * @param fallback             codec for every field this codec does not write as a run table
     * @param fieldContextResolver tells whether a field is a dimension; {@code null} disables the
     *                             run-table layout, so every field is written by {@code fallback}
     */
    RunTableSortedSetCodec(final SortedSetOrdinalCodec fallback, @Nullable final FieldContextResolver fieldContextResolver) {
        this.fallback = fallback;
        this.fieldContextResolver = fieldContextResolver;
    }

    @Override
    public SortedSetOrdinalWriter createWriter(final NumericWriteContext ctx) {
        final RunTableGate gate = new RunTableGate(fieldContextResolver, ctx.primarySortFieldNumber(), ctx.maxDoc(), ctx.blockSize());
        return (field, values, maxOrd) -> {
            if (gate.allow(field, maxOrd)) {
                final DocValueFieldCountStats stats = tryWriteRunTable(ctx, gate, field, values, maxOrd);
                if (stats != null) {
                    return stats;
                }
            }
            ctx.meta().writeByte(RunTableLayout.LAYOUT_DEFAULT);
            // Reading the values a second time after an abandoned attempt is safe: TsdbDocValuesProducer
            // hands out a fresh iterator on every getSortedNumeric call.
            return fallback.createWriter(ctx).writeOrdinals(field, values, maxOrd);
        };
    }

    /**
     * Walks the field's docs, collapsing them into runs, and writes the run table if the gate stays open.
     * Returns {@code null}, having written nothing, otherwise.
     */
    @Nullable
    private static DocValueFieldCountStats tryWriteRunTable(
        final NumericWriteContext ctx,
        final RunTableGate gate,
        final FieldInfo field,
        final TsdbDocValuesProducer values,
        long maxOrd
    ) throws IOException {
        final int maxDoc = ctx.maxDoc();
        final RunTableSortedSetOrdinalWriter runTable = new RunTableSortedSetOrdinalWriter(Math.toIntExact(maxOrd));
        final SortedNumericDocValues ords = values.getSortedNumeric(field);
        int[] set = new int[8];
        int numDocsWithField = 0;
        long numValues = 0;
        int present = ords.nextDoc();
        for (int doc = 0; doc < maxDoc; doc++) {
            if (doc == present) {
                final int count = ords.docValueCount();
                set = ArrayUtil.grow(set, count);
                for (int i = 0; i < count; i++) {
                    set[i] = Math.toIntExact(ords.nextValue());
                }
                runTable.add(set, count);
                numDocsWithField++;
                numValues += count;
                present = ords.nextDoc();
            } else {
                runTable.add(set, 0);
            }
            if (gate.allow(runTable.numRuns(), doc + 1) == false) {
                return null;
            }
        }
        ctx.meta().writeByte(RunTableLayout.LAYOUT_RUN_TABLE);
        SortedSetRunTableLayout.encode(runTable, numDocsWithField, numValues, ctx.data(), ctx.meta());
        return new DocValueFieldCountStats(numDocsWithField, numValues);
    }

    @Override
    public SortedSetOrdinalReader readOrdinalMeta(
        final NumericReadContext ctx,
        final IndexInput meta,
        final SortedNumericEntry entry,
        int numericBlockShift
    ) throws IOException {
        final byte layout = meta.readByte();
        return switch (layout) {
            case RunTableLayout.LAYOUT_RUN_TABLE -> {
                final SortedSetRunTableLayout.Meta runTable = SortedSetRunTableLayout.readMeta(meta);
                // Merges sum these counts across segments, both to decide whether the merged field is
                // single-valued and to size its layout, so they must be exact.
                entry.numDocsWithField = runTable.numDocsWithField();
                entry.numValues = runTable.numValues();
                // The run table encodes missing docs as empty runs and writes no docs-with-field set or
                // addresses table.
                entry.docsWithFieldOffset = -1;
                entry.blockSize = 1 << numericBlockShift;
                yield new RunTableReader(runTable);
            }
            case RunTableLayout.LAYOUT_DEFAULT -> fallback.readOrdinalMeta(ctx, meta, entry, numericBlockShift);
            default -> throw new CorruptIndexException("invalid sorted set ordinal layout [" + layout + "]", meta);
        };
    }

    /**
     * Opens the ordinals of one field written in the run-table layout. It is a named type, rather than a
     * lambda, so tests can tell which layout a field was read from.
     */
    record RunTableReader(SortedSetRunTableLayout.Meta meta) implements SortedSetOrdinalReader {
        @Override
        public SortedNumericDocValues ordinals(final OrdinalReadContext readCtx, long maxOrd) throws IOException {
            return SortedSetRunTableLayout.open(meta, readCtx.data(), readCtx.maxDoc());
        }
    }
}
