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
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesConsumer.DocValueCountConsumer;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.NumericEntry;
import org.elasticsearch.index.codec.tsdb.DocValueFieldCountStats;
import org.elasticsearch.index.codec.tsdb.NumericReadContext;
import org.elasticsearch.index.codec.tsdb.NumericWriteContext;
import org.elasticsearch.index.codec.tsdb.SortedFieldObserver;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalCodec;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalMergeWriter;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalReader;
import org.elasticsearch.index.codec.tsdb.SortedOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.SortedRunView;
import org.elasticsearch.index.codec.tsdb.TsdbDocValuesProducer;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedOrdinalReader;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedOrdinalWriter;
import org.elasticsearch.index.codec.tsdb.es95.runtable.RunTableSortedSeriesCursor;
import org.elasticsearch.index.codec.tsdb.es95.runtable.ScanningSeriesIterator;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SeriesIterator;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedRunMerger;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedRunTableLayout;
import org.elasticsearch.index.codec.tsdb.es95.runtable.SortedSeriesCursor;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/**
 * {@link SortedOrdinalCodec} that writes and reads the run-table ordinal layout for single-valued dimension
 * fields, composing a fallback {@link SortedOrdinalCodec} for everything it does not encode itself.
 *
 * <p>TSDB segments are index-sorted by {@code (_tsid, @timestamp)}, so a non-primary-sort dimension's
 * per-doc ordinal stream is piecewise-constant: it changes value only at series boundaries. The
 * run-table layout collapses that stream into one entry per run, so storage scales with the number of
 * series rather than the number of docs. It applies to a single-valued, non-primary-sort field, whether
 * dense or sparse: absent docs are represented by the reserved sentinel ordinal {@code K} (the field
 * cardinality) and form sentinel runs, so a dimension absent for whole contiguous series adds only a few
 * runs. It is selected when an average run spans at least two docs over the full doc space; every other
 * case (the primary-sort field, single-ordinal fields, multi-valued fields, scattered absence, and
 * degenerate near-churn where runs approach docs) delegates to the {@code fallback} codec, which owns the
 * range, single-ordinal, and blocked layouts. The sentinel is internal to the ordinal stream and never
 * enters the terms dictionary, which holds only the {@code K} real ordinals.
 *
 * <p>The layout is self-describing: a discriminator byte at the front of each field's ordinal metadata
 * records whether the run-table or the fallback layout follows. The discriminator is always written and
 * always read, so this codec is installed on every ES95 segment: the read path stays correct even when
 * opened by a no-argument format instance (for example, during segment reading after a codec upgrade).
 */
final class RunTableSortedCodec implements SortedOrdinalCodec {

    private final SortedOrdinalCodec fallback;
    private final IntFunction<RunTableSortedOrdinalWriter> accumulatorFactory;
    @Nullable
    private final FieldContextResolver fieldContextResolver;

    /**
     * @param fallback             codec that owns every layout other than run-table and every field
     *                             run-table does not encode itself
     * @param accumulatorFactory   creates the accumulator for each field write; receives the sentinel
     *                             value count and returns a fresh writer
     * @param fieldContextResolver resolves whether a field is a TSDB dimension at write time;
     *                             {@code null} disables the run-table path entirely and falls back
     *                             to the baseline layout for every field
     */
    RunTableSortedCodec(
        final SortedOrdinalCodec fallback,
        final IntFunction<RunTableSortedOrdinalWriter> accumulatorFactory,
        @Nullable final FieldContextResolver fieldContextResolver
    ) {
        this.fallback = fallback;
        this.accumulatorFactory = accumulatorFactory;
        this.fieldContextResolver = fieldContextResolver;
    }

    @Override
    public SortedOrdinalWriter createWriter(final NumericWriteContext ctx) {
        return new RunTableSortedWriter(
            ctx,
            fallback,
            accumulatorFactory,
            new RunTableGate(fieldContextResolver, ctx.primarySortFieldNumber(), ctx.maxDoc(), ctx.blockSize())
        );
    }

    @Override
    public SortedOrdinalReader createReader(final NumericReadContext ctx, final IndexInput data, int maxDoc) {
        return new RunTableSortedReader(fallback.createReader(ctx, data, maxDoc), data, maxDoc);
    }

    @Override
    public SortedOrdinalMergeWriter createMergeWriter() {
        return new RunTableSortedMergeWriter(fieldContextResolver);
    }

    private static final class RunTableSortedWriter implements SortedOrdinalWriter {

        private final NumericWriteContext ctx;
        private final SortedOrdinalCodec fallback;
        private final IntFunction<RunTableSortedOrdinalWriter> accumulatorFactory;
        private final RunTableGate policy;

        RunTableSortedWriter(
            final NumericWriteContext ctx,
            final SortedOrdinalCodec fallback,
            final IntFunction<RunTableSortedOrdinalWriter> accumulatorFactory,
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
            final int sentinel = Math.toIntExact(maxOrd);
            final RunTableSortedOrdinalWriter runTable = accumulatorFactory.apply(sentinel);
            final SortedNumericDocValues ords = values.getSortedNumeric(field);
            int numDocsWithField = 0;
            long numValues = 0;
            boolean multiValued = false;
            int present = ords.nextDoc();
            for (int doc = 0; doc < maxDoc; doc++) {
                if (doc == present) {
                    if (ords.docValueCount() != 1) {
                        multiValued = true;
                        break;
                    }
                    runTable.add((int) ords.nextValue());
                    numDocsWithField++;
                    numValues++;
                    present = ords.nextDoc();
                } else {
                    runTable.add(sentinel);
                }
                if (policy.allow(runTable.numRuns(), doc + 1) == false) {
                    return writeDefault(field, values, maxOrd, docValueCountConsumer, sortedFieldObserver);
                }
            }

            if (multiValued) {
                return writeDefault(field, values, maxOrd, docValueCountConsumer, sortedFieldObserver);
            }

            ctx.meta().writeByte(RunTableLayout.LAYOUT_RUN_TABLE);
            SortedRunTableLayout.encode(runTable, ctx.data(), ctx.meta());

            return new DocValueFieldCountStats(numDocsWithField, numValues, false);
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

    private static final class RunTableSortedReader implements SortedOrdinalReader {

        private final SortedOrdinalReader fallbackReader;
        private final IndexInput data;
        private final int maxDoc;

        RunTableSortedReader(final SortedOrdinalReader fallbackReader, final IndexInput data, int maxDoc) {
            this.fallbackReader = fallbackReader;
            this.data = data;
            this.maxDoc = maxDoc;
        }

        @Override
        public void readOrdinalMeta(final IndexInput meta, final NumericEntry entry, int numericBlockShift) throws IOException {
            final byte layout = meta.readByte();
            switch (layout) {
                case RunTableLayout.LAYOUT_RUN_TABLE -> {
                    entry.runTableMeta = SortedRunTableLayout.readMeta(meta);
                    entry.docsWithFieldOffset = -1;
                    entry.blockSize = 1 << numericBlockShift;
                }
                case RunTableLayout.LAYOUT_DEFAULT -> fallbackReader.readOrdinalMeta(meta, entry, numericBlockShift);
                default -> throw new CorruptIndexException("Invalid run-table layout discriminator: " + layout, meta);
            }
        }

        @Override
        public NumericDocValues ordinals(final NumericEntry entry, long maxOrd) throws IOException {
            if (entry.runTableMeta != null) {
                final RunTableSortedOrdinalReader.Meta meta = (RunTableSortedOrdinalReader.Meta) entry.runTableMeta;
                return SortedRunTableLayout.open(meta, data.clone(), maxDoc);
            }
            return fallbackReader.ordinals(entry, maxOrd);
        }

        @Override
        public SortedRunView runs(final NumericEntry entry, long maxOrd) throws IOException {
            if (entry.runTableMeta != null) {
                final RunTableSortedOrdinalReader.Meta meta = (RunTableSortedOrdinalReader.Meta) entry.runTableMeta;
                return SortedRunTableLayout.openRuns(meta, data.clone(), maxDoc);
            }
            return fallbackReader.runs(entry, maxOrd);
        }
    }

    /**
     * Produces a merged field's run-table ordinal columns from the source segments' runs, the merge-time
     * counterpart of {@link RunTableSortedWriter}. It applies the same eligibility gate as flush and requires
     * every source segment to have stored the field run-table encoded; when either fails it returns {@code false}
     * so the consumer performs the per-doc merge. On success it emits the same discriminator byte and layout the
     * flush writer emits, so merged output is byte-identical to a per-doc re-encode.
     */
    private static final class RunTableSortedMergeWriter implements SortedOrdinalMergeWriter {

        @Nullable
        private final FieldContextResolver fieldContextResolver;

        RunTableSortedMergeWriter(@Nullable final FieldContextResolver fieldContextResolver) {
            this.fieldContextResolver = fieldContextResolver;
        }

        @Override
        public boolean writeMergedOrdinals(
            final FieldInfo field,
            final MergeState mergeState,
            final OrdinalMap ordinalMap,
            final NumericWriteContext ctx,
            long maxOrd
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

            final int mergedSentinel = Math.toIntExact(maxOrd);
            final List<SortedSeriesCursor> cursors = new ArrayList<>(numSegments);
            for (int i = 0; i < numSegments; i++) {
                final SortedRunView fieldRuns = producers[i].getSortedRunView(sourceFields[i]);
                if (fieldRuns == null) {
                    return false;
                }
                final SeriesIterator series = new ScanningSeriesIterator(
                    producers[i].getSorted(sourceTsidFields[i]),
                    tsidOrdinalMap.getGlobalOrds(i),
                    mergeState.maxDocs[i]
                );
                cursors.add(new RunTableSortedSeriesCursor(series, fieldRuns, ordinalMap.getGlobalOrds(i), mergedSentinel));
            }

            final RunTableSortedOrdinalWriter accumulator = new RunTableSortedOrdinalWriter(mergedSentinel);
            SortedRunMerger.merge(cursors, accumulator);

            ctx.meta().writeByte(RunTableLayout.LAYOUT_RUN_TABLE);
            SortedRunTableLayout.encode(accumulator, ctx.data(), ctx.meta());
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
