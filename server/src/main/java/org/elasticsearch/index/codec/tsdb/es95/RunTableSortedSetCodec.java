/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer.DocValueCountConsumer;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.SortedNumericEntry;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.DocValuesConsumerUtil;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalMergeWriter;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedSetOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.SortedSetRunView;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSetOrdinalReader;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSetOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSetSeriesCursor;
import org.elasticsearch.index.codec.tsdb.es95.runtable.ScanningSeriesIterator;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SeriesIterator;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedSetRunMerger;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedSetRunTableLayout;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedSetSeriesCursor;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * {@link SortedSetOrdinalCodec} that writes and reads the run-table ordinal layout for multi-valued dimension
 * fields, composing a fallback {@link SortedSetOrdinalCodec} for everything it does not encode itself. It is the
 * multi-valued counterpart of {@link RunTableSortedCodec} and shares its discriminator byte and selection
 * bar through {@link RunTableLayout}; the two codecs and their interfaces stay separate.
 *
 * <p>TSDB segments are index-sorted by {@code (_tsid, @timestamp)}, so a non-primary-sort dimension's
 * per-doc ordinal set is piecewise-constant: {@code _tsid} hashes every element of a multi-valued
 * dimension, so a differing set means a different series. The run-table layout collapses that stream into
 * one entry per run, so storage scales with the number of series rather than the number of docs. It applies
 * whether dense or sparse: a doc with no values is the empty set, which forms empty runs, so a dimension
 * absent for whole contiguous series adds only a few runs. It is selected when an average run spans at least
 * two docs over the full doc space; scattered absence and degenerate near-churn delegate to the
 * {@code fallback} codec, which owns the baseline blocked layout. The empty set is internal to the ordinal
 * stream and never enters the terms dictionary, which holds only the {@code K} real ordinals.
 *
 * <p>The default multi-valued {@code SortedSet} layout carries a per-doc addresses table (a
 * doc-to-start-index into the flattened ord stream) that the shared consumer writes around the ordinal
 * codec, gated on whether the field has more values than docs. The run-table layout does not need it: its
 * per-run {@code setOffset[]} column already delimits each doc's ord slice, and the reader reconstructs
 * each doc's set directly from its own {@code startDoc[]}/{@code setOffset[]}/{@code ordStream[]} columns.
 * The writer therefore leaves the offsets accumulator unfed and returns
 * {@link DocValueFieldCountStats#skipAddressesTable() skipAddressesTable},
 * which tells the shared consumer to skip building and writing the addresses table; the reader detects the
 * run-table discriminator and skips reading it. The default layout still writes and reads the table
 * unchanged.
 *
 * <p>The layout is self-describing: a discriminator byte at the front of each field's ordinal metadata
 * records whether the run-table or the fallback layout follows. The discriminator is always written and
 * always read, so this codec is installed on every ES95 segment: the read path stays correct even when
 * opened by a no-argument format instance.
 */
final class RunTableSortedSetCodec implements SortedSetOrdinalCodec {

    private final SortedSetOrdinalCodec fallback;
    private final IntFunction<RunTableSortedSetOrdinalWriter> accumulatorFactory;
    @Nullable
    private final FieldContextResolver fieldContextResolver;

    /**
     * @param fallback             codec that owns every layout other than run-table and every field
     *                             run-table does not encode itself
     * @param accumulatorFactory   creates the accumulator for each field write; receives the field
     *                             cardinality and returns a fresh writer
     * @param fieldContextResolver resolves whether a field is a TSDB dimension at write time;
     *                             {@code null} disables the run-table path entirely and falls back
     *                             to the baseline layout for every field
     */
    RunTableSortedSetCodec(
        final SortedSetOrdinalCodec fallback,
        final IntFunction<RunTableSortedSetOrdinalWriter> accumulatorFactory,
        @Nullable final FieldContextResolver fieldContextResolver
    ) {
        this.fallback = fallback;
        this.accumulatorFactory = accumulatorFactory;
        this.fieldContextResolver = fieldContextResolver;
    }

    @Override
    public SortedSetOrdinalWriter createWriter(final NumericWriteContext ctx) {
        return new RunTableSortedSetWriter(
            ctx,
            fallback,
            accumulatorFactory,
            new RunTableGate(fieldContextResolver, ctx.primarySortFieldNumber(), ctx.maxDoc(), ctx.blockSize())
        );
    }

    @Override
    public SortedSetOrdinalReader createReader(final NumericReadContext ctx, final IndexInput data, int maxDoc) {
        return new RunTableSortedSetReader(fallback.createReader(ctx, data, maxDoc), data, maxDoc);
    }

    @Override
    public SortedSetOrdinalMergeWriter createMergeWriter() {
        return new RunTableSortedSetMergeWriter(fieldContextResolver);
    }

    private static final class RunTableSortedSetWriter implements SortedSetOrdinalWriter {

        private static final int[] EMPTY = new int[0];

        private final NumericWriteContext ctx;
        private final SortedSetOrdinalCodec fallback;
        private final IntFunction<RunTableSortedSetOrdinalWriter> accumulatorFactory;
        private final RunTableGate policy;

        RunTableSortedSetWriter(
            final NumericWriteContext ctx,
            final SortedSetOrdinalCodec fallback,
            final IntFunction<RunTableSortedSetOrdinalWriter> accumulatorFactory,
            final RunTableGate policy
        ) {
            this.ctx = ctx;
            this.fallback = fallback;
            this.accumulatorFactory = accumulatorFactory;
            this.policy = policy;
        }

        @Override
        public DocValueFieldCountStats writeOrdinals(
            final FieldInfo field,
            final TsdbDocValuesProducer values,
            long maxOrd,
            final DocValueCountConsumer docValueCountConsumer,
            final SortedFieldObserver sortedFieldObserver
        ) throws IOException {
            if (policy.allow(field, maxOrd) == false) {
                return writeDefault(field, values, maxOrd, docValueCountConsumer, sortedFieldObserver);
            }

            final int maxDoc = ctx.maxDoc();
            final int valueCount = Math.toIntExact(maxOrd);
            final RunTableSortedSetOrdinalWriter runTable = accumulatorFactory.apply(valueCount);
            final SortedNumericDocValues ords = values.getSortedNumeric(field);

            int numDocsWithField = 0;
            long numValues = 0;
            int[] set = new int[8];

            int present = ords.nextDoc();
            for (int doc = 0; doc < maxDoc; doc++) {
                if (doc == present) {
                    final int count = ords.docValueCount();
                    set = ArrayUtil.grow(set, count);
                    for (int i = 0; i < count; i++) {
                        set[i] = (int) ords.nextValue();
                    }
                    runTable.add(set, count);
                    numDocsWithField++;
                    numValues += count;
                    present = ords.nextDoc();
                } else {
                    runTable.add(EMPTY);
                }
                if (policy.allow(runTable.numRuns(), doc + 1) == false) {
                    return writeDefault(field, values, maxOrd, docValueCountConsumer, sortedFieldObserver);
                }
            }

            ctx.meta().writeByte(RunTableLayout.LAYOUT_RUN_TABLE);
            ctx.meta().writeVInt(numDocsWithField);
            ctx.meta().writeVLong(numValues);
            SortedSetRunTableLayout.encode(runTable, ctx.data(), ctx.meta());

            return new DocValueFieldCountStats(numDocsWithField, numValues, true);
        }

        private DocValueFieldCountStats writeDefault(
            final FieldInfo field,
            final TsdbDocValuesProducer values,
            long maxOrd,
            final DocValueCountConsumer docValueCountConsumer,
            final SortedFieldObserver sortedFieldObserver
        ) throws IOException {
            ctx.meta().writeByte(RunTableLayout.LAYOUT_DEFAULT);
            // Re-reading the field is safe: TsdbDocValuesProducer is stateless and supports multiple
            // getSortedNumeric calls for the same field within a single flush.
            return fallback.createWriter(ctx).writeOrdinals(field, values, maxOrd, docValueCountConsumer, sortedFieldObserver);
        }
    }

    private static final class RunTableSortedSetReader implements SortedSetOrdinalReader {

        private final SortedSetOrdinalReader fallbackReader;
        private final IndexInput data;
        private final int maxDoc;

        RunTableSortedSetReader(final SortedSetOrdinalReader fallbackReader, final IndexInput data, int maxDoc) {
            this.fallbackReader = fallbackReader;
            this.data = data;
            this.maxDoc = maxDoc;
        }

        @Override
        public void readOrdinalMeta(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
            final byte layout = meta.readByte();
            switch (layout) {
                case RunTableLayout.LAYOUT_RUN_TABLE -> {
                    entry.numDocsWithField = meta.readVInt();
                    entry.numValues = meta.readVLong();
                    entry.runTableMeta = SortedSetRunTableLayout.readMeta(meta);
                    entry.docsWithFieldOffset = -1;
                    entry.blockSize = 1 << numericBlockShift;
                }
                case RunTableLayout.LAYOUT_DEFAULT -> fallbackReader.readOrdinalMeta(meta, entry, numericBlockShift);
                default -> throw new CorruptIndexException("Invalid run-table layout discriminator: " + layout, meta);
            }
        }

        @Override
        public SortedNumericDocValues ordinals(final SortedNumericEntry entry, long maxOrd) throws IOException {
            if (entry.runTableMeta != null) {
                final RunTableSortedSetOrdinalReader.Meta meta = (RunTableSortedSetOrdinalReader.Meta) entry.runTableMeta;
                return SortedSetRunTableLayout.open(meta, data.clone(), maxDoc);
            }
            return fallbackReader.ordinals(entry, maxOrd);
        }

        @Override
        public SortedSetRunView runs(final SortedNumericEntry entry, long maxOrd) throws IOException {
            if (entry.runTableMeta != null) {
                final RunTableSortedSetOrdinalReader.Meta meta = (RunTableSortedSetOrdinalReader.Meta) entry.runTableMeta;
                return SortedSetRunTableLayout.openRuns(meta, data.clone(), maxDoc);
            }
            return fallbackReader.runs(entry, maxOrd);
        }
    }

    /**
     * Produces a merged multi-valued field's run-table ordinal columns from the source segments' runs, the
     * merge-time counterpart of {@link RunTableSortedSetWriter}. It applies the same eligibility gate as flush and
     * requires every source segment to have stored the field run-table encoded; when either fails it returns
     * {@code false} so the consumer performs the per-doc merge. On success it emits the same header
     * ({@code numDocsWithField}, {@code numValues}) and layout the flush writer emits, so merged output is
     * byte-identical to a per-doc re-encode.
     */
    private static final class RunTableSortedSetMergeWriter implements SortedSetOrdinalMergeWriter {

        @Nullable
        private final FieldContextResolver fieldContextResolver;

        RunTableSortedSetMergeWriter(@Nullable final FieldContextResolver fieldContextResolver) {
            this.fieldContextResolver = fieldContextResolver;
        }

        @Override
        public boolean writeMergedOrdinals(
            final FieldInfo field,
            final MergeState mergeState,
            final OrdinalMap ordinalMap,
            final NumericWriteContext ctx,
            long maxOrd,
            final DocValuesConsumerUtil.MergeStats mergeStats
        ) throws IOException {
            final RunTableGate gate = new RunTableGate(fieldContextResolver, ctx.primarySortFieldNumber(), ctx.maxDoc(), ctx.blockSize());
            if (gate.allow(field, maxOrd) == false) {
                return false;
            }
            final FieldInfo mergedTsid = mergeState.mergeFieldInfos.fieldInfo(ctx.primarySortFieldNumber());
            if (mergedTsid == null) {
                return false;
            }
            final String tsidName = mergedTsid.name;
            final int numSegments = mergeState.docValuesProducers.length;

            final AbstractTSDBDocValuesProducer[] producers = new AbstractTSDBDocValuesProducer[numSegments];
            final FieldInfo[] sourceFields = new FieldInfo[numSegments];
            final FieldInfo[] sourceTsidFields = new FieldInfo[numSegments];
            final SortedDocValues[] tsidForOrdinalMap = new SortedDocValues[numSegments];
            for (int i = 0; i < numSegments; i++) {
                final FieldInfo sourceField = mergeState.fieldInfos[i].fieldInfo(field.name);
                final FieldInfo sourceTsid = mergeState.fieldInfos[i].fieldInfo(tsidName);
                if (sourceField == null || sourceTsid == null) {
                    return false;
                }
                final AbstractTSDBDocValuesProducer producer = unwrap(mergeState.docValuesProducers[i], sourceField);
                if (producer == null) {
                    return false;
                }
                producers[i] = producer;
                sourceFields[i] = sourceField;
                sourceTsidFields[i] = sourceTsid;
                tsidForOrdinalMap[i] = producer.getSorted(sourceTsid);
            }

            final OrdinalMap tsidOrdinalMap = OrdinalMap.build(null, tsidForOrdinalMap, PackedInts.DEFAULT);

            final List<SortedSetSeriesCursor> cursors = new ArrayList<>(numSegments);
            for (int i = 0; i < numSegments; i++) {
                final SortedSetRunView fieldRuns = producers[i].getSortedSetRunView(sourceFields[i]);
                if (fieldRuns == null) {
                    return false;
                }
                final SeriesIterator series = new ScanningSeriesIterator(
                    producers[i].getSorted(sourceTsidFields[i]),
                    tsidOrdinalMap.getGlobalOrds(i),
                    mergeState.maxDocs[i]
                );
                cursors.add(new RunTableSortedSetSeriesCursor(series, fieldRuns, ordinalMap.getGlobalOrds(i)));
            }

            final RunTableSortedSetOrdinalWriter accumulator = new RunTableSortedSetOrdinalWriter(Math.toIntExact(maxOrd));
            SortedSetRunMerger.merge(cursors, accumulator);

            ctx.meta().writeByte(RunTableLayout.LAYOUT_RUN_TABLE);
            ctx.meta().writeVInt(mergeStats.sumNumDocsWithField());
            ctx.meta().writeVLong(mergeStats.sumNumValues());
            SortedSetRunTableLayout.encode(accumulator, ctx.data(), ctx.meta());
            return true;
        }

        private static AbstractTSDBDocValuesProducer unwrap(DocValuesProducer producer, final FieldInfo sourceField) {
            if (producer instanceof FilterDocValuesProducer filter) {
                producer = filter.getIn();
            }
            if (producer instanceof XPerFieldDocValuesFormat.FieldsReader perField) {
                final DocValuesProducer wrapped = perField.getDocValuesProducer(sourceField);
                if (wrapped instanceof AbstractTSDBDocValuesProducer tsdb) {
                    return tsdb;
                }
            }
            return null;
        }
    }
}
