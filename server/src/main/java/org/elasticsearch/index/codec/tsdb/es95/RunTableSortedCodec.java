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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.OrdinalReadContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedRunTableLayout;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;

import java.io.IOException;

/**
 * {@link SortedOrdinalCodec} that writes single-valued dimension fields in the run-table layout and
 * delegates every other field to a {@code fallback} codec.
 *
 * <p>TSDB segments are sorted by {@code (_tsid, @timestamp)}, so the ordinal stream of a dimension other
 * than the primary sort field changes only at series boundaries. The run-table layout stores one
 * {@code (startDoc, ordinal)} entry per run instead of one ordinal per doc, so its size grows with the
 * number of series rather than the number of docs. Docs without a value carry the sentinel ordinal
 * {@code K} (the field's value count), which forms runs like any other ordinal and never enters the terms
 * dictionary. {@link RunTableGate} decides which fields are attempted and when to give up.
 *
 * <p>Every field's ordinal metadata starts with a discriminator byte saying which layout follows, so the
 * reader needs no other information to tell them apart. A future change to the run-table layout can take
 * a new discriminator value.
 */
final class RunTableSortedCodec implements SortedOrdinalCodec {

    /** Discriminator: the fallback codec wrote this field's ordinal stream. */
    static final byte LAYOUT_DEFAULT = 0;
    /** Discriminator: the run-table layout follows. */
    static final byte LAYOUT_RUN_TABLE = 1;

    private final SortedOrdinalCodec fallback;
    @Nullable
    private final FieldContextResolver fieldContextResolver;

    /**
     * @param fallback             codec for every field this codec does not write as a run table
     * @param fieldContextResolver tells whether a field is a dimension; {@code null} disables the
     *                             run-table layout, so every field is written by {@code fallback}
     */
    RunTableSortedCodec(final SortedOrdinalCodec fallback, @Nullable final FieldContextResolver fieldContextResolver) {
        this.fallback = fallback;
        this.fieldContextResolver = fieldContextResolver;
    }

    @Override
    public SortedOrdinalWriter createWriter(final NumericWriteContext ctx) {
        final RunTableGate gate = new RunTableGate(fieldContextResolver, ctx.primarySortFieldNumber(), ctx.maxDoc(), ctx.blockSize());
        return (field, values, maxOrd, sortedFieldObserver) -> {
            if (gate.allow(field, maxOrd)) {
                final DocValueFieldCountStats stats = tryWriteRunTable(ctx, gate, field, values, maxOrd);
                if (stats != null) {
                    // Only the primary sort field gets an observer, and the gate never admits it.
                    assert sortedFieldObserver == SortedFieldObserver.NOOP : "run table cannot feed a sorted field observer";
                    return stats;
                }
            }
            ctx.meta().writeByte(LAYOUT_DEFAULT);
            // Reading the values a second time after an abandoned attempt is safe: TsdbDocValuesProducer
            // hands out a fresh iterator on every getSortedNumeric call.
            return fallback.createWriter(ctx).writeOrdinals(field, values, maxOrd, sortedFieldObserver);
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
        final int sentinel = Math.toIntExact(maxOrd);
        final RunTableSortedOrdinalWriter runTable = new RunTableSortedOrdinalWriter(sentinel);
        final SortedNumericDocValues ords = values.getSortedNumeric(field);
        int numDocsWithField = 0;
        int present = ords.nextDoc();
        for (int doc = 0; doc < maxDoc; doc++) {
            if (doc == present) {
                // The consumer only hands this codec single-valued fields: multi-valued SortedSet fields go to
                // the SortedSet codec.
                assert ords.docValueCount() == 1 : "field [" + field.name + "] is multi-valued";
                runTable.add(Math.toIntExact(ords.nextValue()));
                numDocsWithField++;
                present = ords.nextDoc();
            } else {
                runTable.add(sentinel);
            }
            if (gate.allow(runTable.numRuns(), doc + 1) == false) {
                return null;
            }
        }
        ctx.meta().writeByte(LAYOUT_RUN_TABLE);
        SortedRunTableLayout.encode(runTable, numDocsWithField, ctx.data(), ctx.meta());
        return new DocValueFieldCountStats(numDocsWithField, numDocsWithField);
    }

    @Override
    public SortedOrdinalReader readOrdinalMeta(
        final NumericReadContext ctx,
        final IndexInput meta,
        final NumericEntry entry,
        int numericBlockShift
    ) throws IOException {
        final byte layout = meta.readByte();
        return switch (layout) {
            case LAYOUT_RUN_TABLE -> {
                final SortedRunTableLayout.Meta runTable = SortedRunTableLayout.readMeta(meta);
                // The merge statistics sum these counts across segments, so they must be exact.
                entry.numDocsWithField = runTable.numDocsWithField();
                entry.numValues = runTable.numDocsWithField();
                // The run table encodes missing docs as sentinel runs and writes no docs-with-field set;
                // -1 keeps the producer off its empty-field shortcut, which a run table never needs.
                entry.docsWithFieldOffset = -1;
                entry.blockSize = 1 << numericBlockShift;
                yield new RunTableReader(runTable);
            }
            case LAYOUT_DEFAULT -> fallback.readOrdinalMeta(ctx, meta, entry, numericBlockShift);
            default -> throw new CorruptIndexException("invalid sorted ordinal layout [" + layout + "]", meta);
        };
    }

    /**
     * Opens the ordinals of one field written in the run-table layout. It is a named type, rather than a
     * lambda, so tests can tell which layout a field was read from.
     */
    record RunTableReader(SortedRunTableLayout.Meta meta) implements SortedOrdinalReader {
        @Override
        public NumericDocValues ordinals(final OrdinalReadContext readCtx, long maxOrd) throws IOException {
            return SortedRunTableLayout.open(meta, readCtx.data(), readCtx.maxDoc());
        }
    }
}
