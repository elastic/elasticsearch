/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.Prepared;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileCellIdSource;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

/**
 * A unified interface to different ways of getting input data for
 * {@link Aggregator}s like DocValues from Lucene or script output. The
 * top level sub-classes define type-specific behavior, such as
 * {@link ValuesSource.Numeric#isFloatingPoint()}. Second level subclasses are
 * then specialized based on where they read values from, e.g. script or field
 * cases. There are also adapter classes like {@link GeoTileCellIdSource} which do
 * run-time conversion from one type to another, often dependent on a user
 * specified parameter (precision in that case).
 */
public abstract class ValuesSource {

    /**
     * Get a byte array like view into the values. This is the "native" way
     * to access {@link Bytes}-style values.
     */
    public abstract SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException;

    /**
     * Get a "has any values" view into the values. It'll try to pick the
     * "most native" way to check if there are any values, but it builds its
     * own view into the values so if you need any of the actual values its
     * best to use something like {@link #bytesValues} or
     * {@link Numeric#doubleValues} but if you <strong>just</strong>
     * need to know if there are any values then use this.
     */
    public abstract DocValueBits docsWithValue(LeafReaderContext context) throws IOException;

    /** Whether this values source needs scores. */
    public boolean needsScores() {
        return false;
    }

    /**
     * Build a function to prepare {@link Rounding}s.
     * <p>
     * This returns a {@linkplain Function} because auto date histogram will
     * need to call it many times over the course of running the aggregation.
     * Other aggregations should feel free to call it once.
     */
    protected abstract Function<Rounding, Rounding.Prepared> roundingPreparer() throws IOException;

    /**
     * Check if this values source supports using global and segment ordinals.
     * <p>
     * If this returns {@code true} then it is safe to cast it to {@link ValuesSource.Bytes.WithOrdinals}.
     */
    public boolean hasOrdinals() {
        return false;
    }

    /**
     * {@linkplain ValuesSource} for fields who's values are best thought of
     * as byte arrays without any other meaning like {@code keyword} or
     * {@code ip}. Aggregations that operate on these values presume only
     * that {@link DocValueFormat#format(BytesRef)} will correctly convert
     * the resulting {@link BytesRef} into something human readable.
     */
    public abstract static class Bytes extends ValuesSource {

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final SortedBinaryDocValues bytes = bytesValues(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(bytes);
        }

        @Override
        public final Function<Rounding, Rounding.Prepared> roundingPreparer() throws IOException {
            throw new AggregationExecutionException("can't round a [BYTES]");
        }

        /**
         * Specialization of {@linkplain Bytes} who's underlying storage
         * de-duplicates its bytes by storing them in a per-leaf sorted
         * lookup table. Aggregations that are aware of these lookup tables
         * can operate directly on the value's position in the table, know as
         * the "ordinal". They can then later translate the ordinal into
         * the {@link BytesRef} value.
         */
        public abstract static class WithOrdinals extends Bytes {

            public static final WithOrdinals EMPTY = new WithOrdinals() {

                @Override
                public SortedSetDocValues ordinalsValues(LeafReaderContext context) {
                    return DocValues.emptySortedSet();
                }

                @Override
                public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) {
                    return DocValues.emptySortedSet();
                }

                @Override
                public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                    return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
                }

                @Override
                public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException {
                    return LongUnaryOperator.identity();
                }

            };

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                final SortedSetDocValues ordinals = ordinalsValues(context);
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(ordinals);
            }

            /**
             * Get a view into the leaf's ordinals and their {@link BytesRef} values.
             * <p>
             * Use {@link SortedSetDocValues#advanceExact}, {@link SortedSetDocValues#getValueCount},
             * and {@link SortedSetDocValues#nextOrd} to fetch the ordinals. Use
             * {@link SortedSetDocValues#lookupOrd} to convert form the ordinal
             * number into the {@link BytesRef} value. Make sure to
             * {@link BytesRef#deepCopyOf(BytesRef) copy} the result if you need
             * to keep it.
             * <p>
             * Each leaf may have a different ordinal for the same byte array.
             * Imagine, for example, an index where one leaf has the values
             * {@code "a", "b", "d"} and another leaf has the values
             * {@code "b", "c", "d"}. {@code "a"} has the ordinal {@code 0} in
             * the first leaf and doesn't exist in the second leaf.
             * {@code "b"} has the ordinal {@code 1} in the first leaf
             * and {@code 0} in the second leaf. {@code "c"} doesn't exist in
             * the first leaf and has the ordinal {@code 1} in the second leaf.
             * And {@code "d"} gets the ordinal {@code 2} in both leaves.
             * <p>
             * If you have to compare the ordinals of values from different
             * segments then you'd need to somehow merge them. {@link #globalOrdinalsValues}
             * provides such a merging at the cost of longer startup times when
             * the index has been modified.
             */
            public abstract SortedSetDocValues ordinalsValues(LeafReaderContext context) throws IOException;

            /**
             * Get a "global" view into the leaf's ordinals. This can require
             * construction of fairly large set of lookups in memory so prefer
             * {@link #ordinalsValues} unless you need the global view.
             * <p>
             * This functions just like {@link #ordinalsValues} except that the
             * ordinals that {@link SortedSetDocValues#nextOrd} and
             * {@link SortedSetDocValues#lookupOrd(long)} operate on are "global"
             * to all segments in the shard. They are ordinals into a lookup
             * table containing all values on the shard.
             * <p>
             * Compare this to the example in the docs for {@link #ordinalsValues}.
             * Imagine, again, an index where one leaf has the values
             * {@code "a", "b", "d"} and another leaf has the values
             * {@code "b", "c", "d"}. The global ordinal for {@code "a"} is {@code 0}.
             * The global ordinal for {@code "b"} is {@code 1}. The global ordinal
             * for {@code "c"} is {@code 2}. And the global ordinal for {@code "d"}
             * is, you guessed it, {@code 3}.
             * <p>
             * This makes comparing the values from different segments much simpler.
             * But it comes with a fairly high memory cost and a substantial
             * performance hit when this method is first called after modifying the index.
             * If the global ordinals lookup hasn't been built then this method's runtime
             * is roughly proportional to the number of distinct values on the field.
             * If there are very few distinct values then the runtime'll be dominated
             * by factors related to the number of segments. But in that case it'll
             * be fast enough that you won't usually care.
             */
            public abstract SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) throws IOException;

            /**
             * Whether this values source is able to provide a mapping between global and segment ordinals,
             * by returning the underlying {@link OrdinalMap}. If this method returns false, then calling
             * {@link #globalOrdinalsMapping} will result in an {@link UnsupportedOperationException}.
             */
            public boolean supportsGlobalOrdinalsMapping() {
                return true;
            }

            @Override
            public boolean hasOrdinals() {
                return true;
            }

            /**
             * Returns a mapping from segment ordinals to global ordinals. This
             * allows you to post process segment ordinals into global ordinals
             * which could save you a few lookups. Also, operating on segment
             * ordinals is likely to produce a more "dense" list of, say, counts.
             * <p>
             * Anyone looking to use this strategy rather than looking up on the
             * fly should benchmark well and update this documentation with what
             * they learn.
             */
            public abstract LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException;

            /**
             * Get the maximum global ordinal. Requires {@link #globalOrdinalsValues}
             * so see the note about its performance.
             */
            public long globalMaxOrd(IndexSearcher indexSearcher) throws IOException {
                IndexReader indexReader = indexSearcher.getIndexReader();
                if (indexReader.leaves().isEmpty()) {
                    return 0;
                } else {
                    LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
                    SortedSetDocValues values = globalOrdinalsValues(atomicReaderContext);
                    return values.getValueCount();
                }
            }

            public static class FieldData extends WithOrdinals {

                protected final IndexOrdinalsFieldData indexFieldData;

                public FieldData(IndexOrdinalsFieldData indexFieldData) {
                    this.indexFieldData = indexFieldData;
                }

                @Override
                public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                    final LeafOrdinalsFieldData atomicFieldData = indexFieldData.load(context);
                    return atomicFieldData.getBytesValues();
                }

                @Override
                public SortedSetDocValues ordinalsValues(LeafReaderContext context) {
                    final LeafOrdinalsFieldData atomicFieldData = indexFieldData.load(context);
                    return atomicFieldData.getOrdinalsValues();
                }

                @Override
                public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) {
                    final IndexOrdinalsFieldData global = indexFieldData.loadGlobal((DirectoryReader)context.parent.reader());
                    final LeafOrdinalsFieldData atomicFieldData = global.load(context);
                    return atomicFieldData.getOrdinalsValues();
                }

                @Override
                public boolean supportsGlobalOrdinalsMapping() {
                    return indexFieldData.supportsGlobalOrdinalsMapping();
                }

                @Override
                public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException {
                    final IndexOrdinalsFieldData global = indexFieldData.loadGlobal((DirectoryReader)context.parent.reader());
                    final OrdinalMap map = global.getOrdinalMap();
                    if (map == null) {
                        // segments and global ordinals are the same
                        return LongUnaryOperator.identity();
                    }
                    final org.apache.lucene.util.LongValues segmentToGlobalOrd = map.getGlobalOrds(context.ord);
                    return segmentToGlobalOrd::get;
                }
            }
        }

        public static class FieldData extends Bytes {

            protected final IndexFieldData<?> indexFieldData;

            public FieldData(IndexFieldData<?> indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

        }

        /**
         * {@link ValuesSource} implementation for stand alone scripts returning a Bytes value
         */
        public static class Script extends Bytes {

            private final AggregationScript.LeafFactory script;

            public Script(AggregationScript.LeafFactory script) {
                this.script = script;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.newInstance(context));
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }
        }

        // No need to implement ReaderContextAware here, the delegate already takes care of updating data structures
        /**
         * {@link ValuesSource} subclass for Bytes fields with a Value Script applied
         */
        public static class WithScript extends Bytes {

            private final ValuesSource delegate;
            private final AggregationScript.LeafFactory script;

            public WithScript(ValuesSource delegate, AggregationScript.LeafFactory script) {
                this.delegate = delegate;
                this.script = script;
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new BytesValues(delegate.bytesValues(context), script.newInstance(context));
            }

            static class BytesValues extends SortingBinaryDocValues implements ScorerAware {

                private final SortedBinaryDocValues bytesValues;
                private final AggregationScript script;

                BytesValues(SortedBinaryDocValues bytesValues, AggregationScript script) {
                    this.bytesValues = bytesValues;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    if (bytesValues.advanceExact(doc)) {
                        count = bytesValues.docValueCount();
                        grow();
                        script.setDocument(doc);
                        for (int i = 0; i < count; ++i) {
                            final BytesRef value = bytesValues.nextValue();
                            script.setNextAggregationValue(value.utf8ToString());
                            Object run = script.execute();
                            CollectionUtils.ensureNoSelfReferences(run, "ValuesSource.BytesValues script");
                            values[i].copyChars(run.toString());
                        }
                        sort();
                        return true;
                    } else {
                        count = 0;
                        grow();
                        return false;
                    }
                }
            }
        }
    }

    /**
     * {@linkplain ValuesSource} for fields who's values are best thought of
     * as numbers. Aggregations that operate on these values often may chose
     * to operate on double precision floating point
     * {@link Numeric#doubleValues values} or on 64 bit signed two's complement
     * {@link Numeric#longValues values}. They'll do normal "number stuff"
     * to those values like add, multiply, and compare them to other numbers.
     */
    public abstract static class Numeric extends ValuesSource {

        public static final Numeric EMPTY = new Numeric() {

            @Override
            public boolean isFloatingPoint() {
                return false;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) {
                return DocValues.emptySortedNumeric();
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedNumericDoubles();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
            }

        };

        /**
         * Are values of this field better represented as a double precision
         * floating point numbers ({@code true}) or 64 bit signed
         * numbers ({@code false})?
         * <p>
         * Aggregations may, if they feel it is important, use this to pick
         * which of {@link #longValues} and {@link #doubleValues} is better for
         * the field values. Most metric aggregations are quite happy to operate
         * on floating point numbers all the time and never call this. Bucketing
         * aggregations that want to enumerate all values
         * (like {@link TermsAggregator}) will want to check this but bucketing
         * aggregations that just compare values ({@link RangeAggregator}) are,
         * like metric aggregators, fine ignoring it.
         */
        public abstract boolean isFloatingPoint();

        /**
         * Get a 64 bit signed view into the values in this leaf.
         * <p>
         * If the values have precision beyond the decimal point then they'll be
         * <a href="https://docs.oracle.com/javase/specs/jls/se15/html/jls-5.html#jls-5.1.3">"narrowed"</a>
         * but they'll accurately represent values up to {@link Long#MAX_VALUE}.
         */
        public abstract SortedNumericDocValues longValues(LeafReaderContext context) throws IOException;

        /**
         * Get a double precision floating point view into the values in this leaf.
         * <p>
         * These values will preserve any precision beyond the decimal point but
         * are limited to {@code double}'s standard 53 bit mantissa. If the "native"
         * field has values that can't be accurately represented in those 53 bits
         * they'll be <a href="https://docs.oracle.com/javase/specs/jls/se15/html/jls-5.html#jls-5.1.2">"widened"</a>
         */
        public abstract SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException;

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            // We try and pick the lowest overhead implementation.
            if (isFloatingPoint()) {
                final SortedNumericDoubleValues values = doubleValues(context);
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(values);
            } else {
                final SortedNumericDocValues values = longValues(context);
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(values);
            }
        }

        @Override
        public Function<Rounding, Prepared> roundingPreparer() throws IOException {
            return Rounding::prepareForUnknown;
        }

        /**
         * {@link ValuesSource} subclass for Numeric fields with a Value Script applied
         */
        public static class WithScript extends Numeric {

            private final Numeric delegate;
            private final AggregationScript.LeafFactory script;

            public WithScript(Numeric delegate, AggregationScript.LeafFactory script) {
                this.delegate = delegate;
                this.script = script;
            }

            @Override
            public boolean isFloatingPoint() {
                return true; // even if the underlying source produces longs, scripts can change them to doubles
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new Bytes.WithScript.BytesValues(delegate.bytesValues(context), script.newInstance(context));
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new LongValues(delegate.longValues(context), script.newInstance(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new DoubleValues(delegate.doubleValues(context), script.newInstance(context));
            }

            static class LongValues extends AbstractSortingNumericDocValues implements ScorerAware {

                private final SortedNumericDocValues longValues;
                private final AggregationScript script;

                LongValues(SortedNumericDocValues values, AggregationScript script) {
                    this.longValues = values;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (longValues.advanceExact(target)) {
                        resize(longValues.docValueCount());
                        script.setDocument(target);
                        for (int i = 0; i < docValueCount(); ++i) {
                            script.setNextAggregationValue(longValues.nextValue());
                            values[i] = script.runAsLong();
                        }
                        sort();
                        return true;
                    }
                    return false;
                }
            }

            static class DoubleValues extends SortingNumericDoubleValues implements ScorerAware {

                private final SortedNumericDoubleValues doubleValues;
                private final AggregationScript script;

                DoubleValues(SortedNumericDoubleValues values, AggregationScript script) {
                    this.doubleValues = values;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (doubleValues.advanceExact(target)) {
                        resize(doubleValues.docValueCount());
                        script.setDocument(target);
                        for (int i = 0; i < docValueCount(); ++i) {
                            script.setNextAggregationValue(doubleValues.nextValue());
                            values[i] = script.runAsDouble();
                        }
                        sort();
                        return true;
                    }
                    return false;
                }
            }
        }

        public static class FieldData extends Numeric {

            protected final IndexNumericFieldData indexFieldData;

            public FieldData(IndexNumericFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public boolean isFloatingPoint() {
                return indexFieldData.getNumericType().isFloatingPoint();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) {
                return indexFieldData.load(context).getLongValues();
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) {
                return indexFieldData.load(context).getDoubleValues();
            }
        }

        /**
         * {@link ValuesSource} implementation for stand alone scripts returning a Numeric value
         */
        public static class Script extends Numeric {
            private final AggregationScript.LeafFactory script;
            private final ValueType scriptValueType;

            public Script(AggregationScript.LeafFactory script, ValueType scriptValueType) {
                this.script = script;
                this.scriptValueType = scriptValueType;
            }

            @Override
            public boolean isFloatingPoint() {
                return scriptValueType != null ? scriptValueType == ValueType.DOUBLE : true;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new ScriptLongValues(script.newInstance(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new ScriptDoubleValues(script.newInstance(context));
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.newInstance(context));
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }
        }
    }

    /**
     * {@linkplain ValuesSource} for fields who's values are best thought of
     * as ranges of numbers, dates, or IP addresses.
     */
    public static class Range extends ValuesSource {
        private final RangeType rangeType;
        protected final IndexFieldData<?> indexFieldData;

        public Range(IndexFieldData<?> indexFieldData, RangeType rangeType) {
            this.indexFieldData = indexFieldData;
            this.rangeType = rangeType;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final SortedBinaryDocValues bytes = bytesValues(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(bytes);
        }

        @Override
        public Function<Rounding, Prepared> roundingPreparer() throws IOException {
            // TODO lookup the min and max rounding when appropriate
            return Rounding::prepareForUnknown;
        }

        public RangeType rangeType() { return rangeType; }
    }

    /**
     * {@linkplain ValuesSource} for fields who's values are best thought of
     * as points on a globe.
     */
    public abstract static class GeoPoint extends ValuesSource {

        public static final GeoPoint EMPTY = new GeoPoint() {

            @Override
            public MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return org.elasticsearch.index.fielddata.FieldData.emptyMultiGeoPoints();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
            }

        };

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final MultiGeoPointValues geoPoints = geoPointValues(context);
            return org.elasticsearch.index.fielddata.FieldData.docsWithValue(geoPoints);
        }

        @Override
        public final Function<Rounding, Rounding.Prepared> roundingPreparer() throws IOException {
            throw new AggregationExecutionException("can't round a [GEO_POINT]");
        }

        public abstract MultiGeoPointValues geoPointValues(LeafReaderContext context);

        public static class Fielddata extends GeoPoint {

            protected final IndexGeoPointFieldData indexFieldData;

            public Fielddata(IndexGeoPointFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            public org.elasticsearch.index.fielddata.MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return indexFieldData.load(context).getGeoPointValues();
            }
        }
    }
}
