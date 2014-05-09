/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.*;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.TopReaderContextAware;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.AtomicFieldData.Order;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.SortedAndUnique.SortedUniqueBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;
import org.elasticsearch.search.internal.SearchContext;

public abstract class ValuesSource {

    public static class MetaData {

        public static final MetaData UNKNOWN = new MetaData();

        public enum Uniqueness {
            UNIQUE,
            NOT_UNIQUE,
            UNKNOWN;

            public boolean unique() {
                return this == UNIQUE;
            }
        }

        private long maxAtomicUniqueValuesCount = -1;
        private boolean multiValued = true;
        private Uniqueness uniqueness = Uniqueness.UNKNOWN;

        private MetaData() {}

        private MetaData(MetaData other) {
            this.maxAtomicUniqueValuesCount = other.maxAtomicUniqueValuesCount;
            this.multiValued = other.multiValued;
            this.uniqueness = other.uniqueness;
        }

        private MetaData(long maxAtomicUniqueValuesCount, boolean multiValued, Uniqueness uniqueness) {
            this.maxAtomicUniqueValuesCount = maxAtomicUniqueValuesCount;
            this.multiValued = multiValued;
            this.uniqueness = uniqueness;
        }

        public long maxAtomicUniqueValuesCount() {
            return maxAtomicUniqueValuesCount;
        }

        public boolean multiValued() {
            return multiValued;
        }

        public Uniqueness uniqueness() {
            return uniqueness;
        }

        public static MetaData load(IndexFieldData indexFieldData, SearchContext context) {
            MetaData metaData = new MetaData();
            metaData.uniqueness = Uniqueness.UNIQUE;
            for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
                AtomicFieldData fieldData = indexFieldData.load(readerContext);
                metaData.multiValued |= fieldData.isMultiValued();
                metaData.maxAtomicUniqueValuesCount = Math.max(metaData.maxAtomicUniqueValuesCount, fieldData.getNumberUniqueValues());
            }
            return metaData;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static Builder builder(MetaData other) {
            return new Builder(other);
        }

        public static class Builder {

            private final MetaData metaData;

            private Builder() {
                metaData = new MetaData();
            }

            private Builder(MetaData metaData) {
                this.metaData = new MetaData(metaData);
            }

            public Builder maxAtomicUniqueValuesCount(long maxAtomicUniqueValuesCount) {
                metaData.maxAtomicUniqueValuesCount = maxAtomicUniqueValuesCount;
                return this;
            }

            public Builder multiValued(boolean multiValued) {
                metaData.multiValued = multiValued;
                return this;
            }

            public Builder uniqueness(Uniqueness uniqueness) {
                metaData.uniqueness = uniqueness;
                return this;
            }

            public MetaData build() {
                return metaData;
            }
        }

    }

    /**
     * Get the current {@link BytesValues}.
     */
    public abstract BytesValues bytesValues();

    /**
     * Ask the underlying data source to provide pre-computed hashes, optional operation.
     */
    public void setNeedsHashes(boolean needsHashes) {}

    public void setNeedsGlobalOrdinals(boolean needsGlobalOrdinals) {}

    public abstract MetaData metaData();

    public static abstract class Bytes extends ValuesSource {

        public static abstract class WithOrdinals extends Bytes implements TopReaderContextAware {

            public abstract BytesValues.WithOrdinals bytesValues();

            public abstract void setNextReader(IndexReaderContext reader);

            public abstract BytesValues.WithOrdinals globalBytesValues();

            public abstract long globalMaxOrd(IndexSearcher indexSearcher);

            public abstract TermsEnum getGlobalTermsEnum();

            public static class FieldData extends WithOrdinals implements ReaderContextAware {

                protected boolean needsHashes;
                protected final IndexFieldData.WithOrdinals<?> indexFieldData;
                protected final MetaData metaData;
                private boolean needsGlobalOrdinals;

                protected AtomicFieldData.WithOrdinals<?> atomicFieldData;
                private BytesValues.WithOrdinals bytesValues;

                protected IndexFieldData.WithOrdinals<?> globalFieldData;
                protected AtomicFieldData.WithOrdinals<?> globalAtomicFieldData;
                private BytesValues.WithOrdinals globalBytesValues;

                private long maxOrd = -1;

                public FieldData(IndexFieldData.WithOrdinals<?> indexFieldData, MetaData metaData) {
                    this.indexFieldData = indexFieldData;
                    this.metaData = metaData;
                    needsHashes = false;
                }

                @Override
                public MetaData metaData() {
                    return metaData;
                }

                public final void setNeedsHashes(boolean needsHashes) {
                    this.needsHashes = needsHashes;
                }

                @Override
                public void setNeedsGlobalOrdinals(boolean needsGlobalOrdinals) {
                    this.needsGlobalOrdinals = needsGlobalOrdinals;
                }

                @Override
                public void setNextReader(AtomicReaderContext reader) {
                    atomicFieldData = indexFieldData.load(reader);
                    if (bytesValues != null) {
                        bytesValues = atomicFieldData.getBytesValues(needsHashes);
                    }
                    if (globalFieldData != null) {
                        globalAtomicFieldData = globalFieldData.load(reader);
                        if (globalBytesValues != null) {
                            globalBytesValues = globalAtomicFieldData.getBytesValues(needsHashes);
                        }
                    }
                }

                @Override
                public BytesValues.WithOrdinals bytesValues() {
                    if (bytesValues == null) {
                        bytesValues = atomicFieldData.getBytesValues(needsHashes);
                    }
                    return bytesValues;
                }

                @Override
                public void setNextReader(IndexReaderContext reader) {
                    if (needsGlobalOrdinals) {
                        globalFieldData = indexFieldData.loadGlobal(reader.reader());
                    }
                }

                @Override
                public BytesValues.WithOrdinals globalBytesValues() {
                    if (globalBytesValues == null) {
                        globalBytesValues = globalAtomicFieldData.getBytesValues(needsHashes);
                    }
                    return globalBytesValues;
                }

                @Override
                public long globalMaxOrd(IndexSearcher indexSearcher) {
                    if (maxOrd != -1) {
                        return maxOrd;
                    }

                    IndexReader indexReader = indexSearcher.getIndexReader();
                    if (indexReader.leaves().isEmpty()) {
                        return maxOrd = 0;
                    } else {
                        AtomicReaderContext atomicReaderContext = indexReader.leaves().get(0);
                        IndexFieldData.WithOrdinals<?> globalFieldData = indexFieldData.loadGlobal(indexReader);
                        AtomicFieldData.WithOrdinals afd = globalFieldData.load(atomicReaderContext);
                        BytesValues.WithOrdinals values = afd.getBytesValues(false);
                        Ordinals.Docs ordinals = values.ordinals();
                        return maxOrd = ordinals.getMaxOrd();
                    }
                }

                @Override
                public TermsEnum getGlobalTermsEnum() {
                    return globalAtomicFieldData.getTermsEnum();
                }
            }

        }

        public static class FieldData extends Bytes implements ReaderContextAware {

            protected boolean needsHashes;
            protected final IndexFieldData<?> indexFieldData;
            protected final MetaData metaData;
            protected AtomicFieldData<?> atomicFieldData;
            private BytesValues bytesValues;

            public FieldData(IndexFieldData<?> indexFieldData, MetaData metaData) {
                this.indexFieldData = indexFieldData;
                this.metaData = metaData;
                needsHashes = false;
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

            public final void setNeedsHashes(boolean needsHashes) {
                this.needsHashes = needsHashes;
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                atomicFieldData = indexFieldData.load(reader);
                if (bytesValues != null) {
                    bytesValues = atomicFieldData.getBytesValues(needsHashes);
                }
            }

            @Override
            public org.elasticsearch.index.fielddata.BytesValues bytesValues() {
                if (bytesValues == null) {
                    bytesValues = atomicFieldData.getBytesValues(needsHashes);
                }
                return bytesValues;
            }
        }

        public static class Script extends Bytes {

            private final ScriptBytesValues values;

            public Script(SearchScript script) {
                values = new ScriptBytesValues(script);
            }

            @Override
            public MetaData metaData() {
                return MetaData.UNKNOWN;
            }

            @Override
            public org.elasticsearch.index.fielddata.BytesValues bytesValues() {
                return values;
            }
        }

        public static class SortedAndUnique extends Bytes implements ReaderContextAware {

            private final ValuesSource delegate;
            private final MetaData metaData;
            private BytesValues bytesValues;

            public SortedAndUnique(ValuesSource delegate) {
                this.delegate = delegate;
                this.metaData = MetaData.builder(delegate.metaData()).uniqueness(MetaData.Uniqueness.UNIQUE).build();
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                bytesValues = null; // order may change per-segment -> reset
            }

            @Override
            public org.elasticsearch.index.fielddata.BytesValues bytesValues() {
                if (bytesValues == null) {
                    bytesValues = delegate.bytesValues();
                    if (bytesValues.isMultiValued() &&
                            (!delegate.metaData().uniqueness.unique() || bytesValues.getOrder() != Order.BYTES)) {
                        bytesValues = new SortedUniqueBytesValues(bytesValues);
                    }
                }
                return bytesValues;
            }

            static class SortedUniqueBytesValues extends BytesValues {
                final BytesValues delegate;
                int[] indices = new int[1]; // at least one
                final BytesRefArray bytes;
                int numUniqueValues;
                int pos = Integer.MAX_VALUE;

                public SortedUniqueBytesValues(BytesValues delegate) {
                    super(delegate.isMultiValued());
                    this.delegate = delegate;
                    bytes = new BytesRefArray(Counter.newCounter(false));
                }

                @Override
                public int setDocument(int docId) {
                    final int numValues = delegate.setDocument(docId);
                    numUniqueValues = 0;
                    pos = 0;
                    if (numValues > 0) {
                        bytes.clear();
                        indices = ArrayUtil.grow(this.indices, numValues);
                        for (int i = 0; i < numValues; ++i) {
                            final BytesRef next = delegate.nextValue();
                            indices[i] = i;
                            bytes.append(next);
                        }
                        numUniqueValues = CollectionUtils.sortAndDedup(bytes, indices);
                    }
                    return numUniqueValues;
                }

                @Override
                public BytesRef nextValue() {
                    bytes.get(scratch, indices[pos++]);
                    return scratch;
                }

                @Override
                public Order getOrder() {
                    return Order.BYTES;
                }

            }

        }

    }

    public static abstract class Numeric extends ValuesSource {

        /** Whether the underlying data is floating-point or not. */
        public abstract boolean isFloatingPoint();

        /** Get the current {@link LongValues}. */
        public abstract LongValues longValues();

        /** Get the current {@link DoubleValues}. */
        public abstract DoubleValues doubleValues();

        public static class WithScript extends Numeric {

            private final LongValues longValues;
            private final DoubleValues doubleValues;
            private final ValuesSource.WithScript.BytesValues bytesValues;

            public WithScript(Numeric delegate, SearchScript script) {
                this.longValues = new LongValues(delegate, script);
                this.doubleValues = new DoubleValues(delegate, script);
                this.bytesValues = new ValuesSource.WithScript.BytesValues(delegate, script);
            }

            @Override
            public boolean isFloatingPoint() {
                return true; // even if the underlying source produces longs, scripts can change them to doubles
            }

            @Override
            public BytesValues bytesValues() {
                return bytesValues;
            }

            @Override
            public LongValues longValues() {
                return longValues;
            }

            @Override
            public DoubleValues doubleValues() {
                return doubleValues;
            }

            @Override
            public MetaData metaData() {
                return MetaData.UNKNOWN;
            }

            static class LongValues extends org.elasticsearch.index.fielddata.LongValues {

                private final Numeric source;
                private final SearchScript script;

                public LongValues(Numeric source, SearchScript script) {
                    super(true);
                    this.source = source;
                    this.script = script;
                }

                @Override
                public int setDocument(int docId) {
                    return source.longValues().setDocument(docId);
                }

                @Override
                public long nextValue() {
                    script.setNextVar("_value", source.longValues().nextValue());
                    return script.runAsLong();
                }
            }

            static class DoubleValues extends org.elasticsearch.index.fielddata.DoubleValues {

                private final Numeric source;
                private final SearchScript script;

                public DoubleValues(Numeric source, SearchScript script) {
                    super(true);
                    this.source = source;
                    this.script = script;
                }

                @Override
                public int setDocument(int docId) {
                    return source.doubleValues().setDocument(docId);
                }

                @Override
                public double nextValue() {
                    script.setNextVar("_value", source.doubleValues().nextValue());
                    return script.runAsDouble();
                }
            }
        }

        public static class FieldData extends Numeric implements ReaderContextAware {

            protected boolean needsHashes;
            protected final IndexNumericFieldData<?> indexFieldData;
            protected final MetaData metaData;
            protected AtomicNumericFieldData atomicFieldData;
            private BytesValues bytesValues;
            private LongValues longValues;
            private DoubleValues doubleValues;

            public FieldData(IndexNumericFieldData<?> indexFieldData, MetaData metaData) {
                this.indexFieldData = indexFieldData;
                this.metaData = metaData;
                needsHashes = false;
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

            @Override
            public boolean isFloatingPoint() {
                return indexFieldData.getNumericType().isFloatingPoint();
            }

            @Override
            public final void setNeedsHashes(boolean needsHashes) {
                this.needsHashes = needsHashes;
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                atomicFieldData = indexFieldData.load(reader);
                if (bytesValues != null) {
                    bytesValues = atomicFieldData.getBytesValues(needsHashes);
                }
                if (longValues != null) {
                    longValues = atomicFieldData.getLongValues();
                }
                if (doubleValues != null) {
                    doubleValues = atomicFieldData.getDoubleValues();
                }
            }

            @Override
            public org.elasticsearch.index.fielddata.BytesValues bytesValues() {
                if (bytesValues == null) {
                    bytesValues = atomicFieldData.getBytesValues(needsHashes);
                }
                return bytesValues;
            }

            @Override
            public org.elasticsearch.index.fielddata.LongValues longValues() {
                if (longValues == null) {
                    longValues = atomicFieldData.getLongValues();
                }
                assert longValues.getOrder() == Order.NUMERIC;
                return longValues;
            }

            @Override
            public org.elasticsearch.index.fielddata.DoubleValues doubleValues() {
                if (doubleValues == null) {
                    doubleValues = atomicFieldData.getDoubleValues();
                }
                assert doubleValues.getOrder() == Order.NUMERIC;
                return doubleValues;
            }
        }

        public static class Script extends Numeric {
            private final ValueType scriptValueType;

            private final ScriptDoubleValues doubleValues;
            private final ScriptLongValues longValues;
            private final ScriptBytesValues bytesValues;

            public Script(SearchScript script, ValueType scriptValueType) {
                this.scriptValueType = scriptValueType;
                longValues = new ScriptLongValues(script);
                doubleValues = new ScriptDoubleValues(script);
                bytesValues = new ScriptBytesValues(script);
            }

            @Override
            public MetaData metaData() {
                return MetaData.UNKNOWN;
            }

            @Override
            public boolean isFloatingPoint() {
                return scriptValueType != null ? scriptValueType.isFloatingPoint() : true;
            }

            @Override
            public LongValues longValues() {
                return longValues;
            }

            @Override
            public DoubleValues doubleValues() {
                return doubleValues;
            }

            @Override
            public BytesValues bytesValues() {
                return bytesValues;
            }

        }

        public static class SortedAndUnique extends Numeric implements ReaderContextAware {

            private final Numeric delegate;
            private final MetaData metaData;
            private LongValues longValues;
            private DoubleValues doubleValues;
            private BytesValues bytesValues;

            public SortedAndUnique(Numeric delegate) {
                this.delegate = delegate;
                this.metaData = MetaData.builder(delegate.metaData()).uniqueness(MetaData.Uniqueness.UNIQUE).build();
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

            @Override
            public boolean isFloatingPoint() {
                return delegate.isFloatingPoint();
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                longValues = null; // order may change per-segment -> reset
                doubleValues = null;
                bytesValues = null;
            }

            @Override
            public org.elasticsearch.index.fielddata.LongValues longValues() {
                if (longValues == null) {
                    longValues = delegate.longValues();
                    if (longValues.isMultiValued() &&
                            (!delegate.metaData().uniqueness.unique() || longValues.getOrder() != Order.NUMERIC)) {
                        longValues = new SortedUniqueLongValues(longValues);
                    }
                }
                return longValues;
            }

            @Override
            public org.elasticsearch.index.fielddata.DoubleValues doubleValues() {
                if (doubleValues == null) {
                    doubleValues = delegate.doubleValues();
                    if (doubleValues.isMultiValued() &&
                            (!delegate.metaData().uniqueness.unique() || doubleValues.getOrder() != Order.NUMERIC)) {
                        doubleValues = new SortedUniqueDoubleValues(doubleValues);
                    }
                }
                return doubleValues;
            }

            @Override
            public org.elasticsearch.index.fielddata.BytesValues bytesValues() {
                if (bytesValues == null) {
                    bytesValues = delegate.bytesValues();
                    if (bytesValues.isMultiValued() &&
                            (!delegate.metaData().uniqueness.unique() || bytesValues.getOrder() != Order.BYTES)) {
                        bytesValues = new SortedUniqueBytesValues(bytesValues);
                    }
                }
                return bytesValues;
            }

            private static class SortedUniqueLongValues extends FilterLongValues {

                int numUniqueValues;
                long[] array = new long[2];
                int pos = Integer.MAX_VALUE;

                protected SortedUniqueLongValues(LongValues delegate) {
                    super(delegate);
                }

                @Override
                public int setDocument(int docId) {
                    final int numValues = super.setDocument(docId);
                    array = ArrayUtil.grow(array, numValues);
                    for (int i = 0; i < numValues; ++i) {
                        array[i] = super.nextValue();
                    }
                    pos = 0;
                    return numUniqueValues = CollectionUtils.sortAndDedup(array, numValues);
                }

                @Override
                public long nextValue() {
                    assert pos < numUniqueValues;
                    return array[pos++];
                }

                @Override
                public Order getOrder() {
                    return Order.NUMERIC;
                }

            }

            private static class SortedUniqueDoubleValues extends FilterDoubleValues {

                private int numUniqueValues;
                private double[] array = new double[2];
                private int pos = Integer.MAX_VALUE;

                SortedUniqueDoubleValues(DoubleValues delegate) {
                    super(delegate);
                }

                @Override
                public int setDocument(int docId) {
                    final int numValues = super.setDocument(docId);
                    array = ArrayUtil.grow(array, numValues);
                    for (int i = 0; i < numValues; ++i) {
                        array[i] = super.nextValue();
                    }
                    pos = 0;
                    return numUniqueValues = CollectionUtils.sortAndDedup(array, numValues);
                }

                @Override
                public double nextValue() {
                    assert pos < numUniqueValues;
                    return array[pos++];
                }

                @Override
                public Order getOrder() {
                    return Order.NUMERIC;
                }

            }

        }

    }

    // No need to implement ReaderContextAware here, the delegate already takes care of updating data structures
    public static class WithScript extends Bytes {

        private final BytesValues bytesValues;

        public WithScript(ValuesSource delegate, SearchScript script) {
            this.bytesValues = new BytesValues(delegate, script);
        }

        @Override
        public MetaData metaData() {
            return MetaData.UNKNOWN;
        }

        @Override
        public BytesValues bytesValues() {
            return bytesValues;
        }

        static class BytesValues extends org.elasticsearch.index.fielddata.BytesValues {

            private final ValuesSource source;
            private final SearchScript script;

            public BytesValues(ValuesSource source, SearchScript script) {
                super(true);
                this.source = source;
                this.script = script;
            }

            @Override
            public int setDocument(int docId) {
                return source.bytesValues().setDocument(docId);
            }

            @Override
            public BytesRef nextValue() {
                BytesRef value = source.bytesValues().nextValue();
                script.setNextVar("_value", value.utf8ToString());
                scratch.copyChars(script.run().toString());
                return scratch;
            }
        }
    }

    public static class GeoPoint extends ValuesSource implements ReaderContextAware {

        protected boolean needsHashes;
        protected final IndexGeoPointFieldData<?> indexFieldData;
        private final MetaData metaData;
        protected AtomicGeoPointFieldData<?> atomicFieldData;
        private BytesValues bytesValues;
        private GeoPointValues geoPointValues;

        public GeoPoint(IndexGeoPointFieldData<?> indexFieldData, MetaData metaData) {
            this.indexFieldData = indexFieldData;
            this.metaData = metaData;
            needsHashes = false;
        }

        @Override
        public MetaData metaData() {
            return metaData;
        }

        @Override
        public final void setNeedsHashes(boolean needsHashes) {
            this.needsHashes = needsHashes;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            atomicFieldData = indexFieldData.load(reader);
            if (bytesValues != null) {
                bytesValues = atomicFieldData.getBytesValues(needsHashes);
            }
            if (geoPointValues != null) {
                geoPointValues = atomicFieldData.getGeoPointValues();
            }
        }

        @Override
        public org.elasticsearch.index.fielddata.BytesValues bytesValues() {
            if (bytesValues == null) {
                bytesValues = atomicFieldData.getBytesValues(needsHashes);
            }
            return bytesValues;
        }

        public org.elasticsearch.index.fielddata.GeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = atomicFieldData.getGeoPointValues();
            }
            return geoPointValues;
        }
    }

}
