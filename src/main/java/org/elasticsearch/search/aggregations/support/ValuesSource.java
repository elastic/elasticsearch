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

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.common.lucene.TopReaderContextAware;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.plain.ParentChildAtomicFieldData;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric.WithScript.DoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource.WithScript.BytesValues;
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

        public static MetaData load(IndexFieldData<?> indexFieldData, SearchContext context) {
            MetaData metaData = new MetaData();
            metaData.uniqueness = Uniqueness.UNIQUE;
            for (AtomicReaderContext readerContext : context.searcher().getTopReaderContext().leaves()) {
                AtomicFieldData fieldData = indexFieldData.load(readerContext);
                if (fieldData instanceof AtomicOrdinalsFieldData) {
                    AtomicOrdinalsFieldData fd = (AtomicOrdinalsFieldData) fieldData;
                    RandomAccessOrds values = fd.getOrdinalsValues();
                    metaData.multiValued |= FieldData.isMultiValued(values);
                    metaData.maxAtomicUniqueValuesCount = Math.max(metaData.maxAtomicUniqueValuesCount, values.getValueCount());
                } else if (fieldData instanceof AtomicNumericFieldData) {
                    AtomicNumericFieldData fd = (AtomicNumericFieldData) fieldData;
                    SortedNumericDoubleValues values = fd.getDoubleValues();
                    metaData.multiValued |= FieldData.isMultiValued(values);
                    metaData.maxAtomicUniqueValuesCount = Long.MAX_VALUE;
                } else if (fieldData instanceof AtomicGeoPointFieldData) {
                    AtomicGeoPointFieldData fd = (AtomicGeoPointFieldData) fieldData;
                    MultiGeoPointValues values = fd.getGeoPointValues();
                    metaData.multiValued |= FieldData.isMultiValued(values);
                    metaData.maxAtomicUniqueValuesCount = Long.MAX_VALUE;
                } else {
                    metaData.multiValued = true;
                    metaData.maxAtomicUniqueValuesCount = Long.MAX_VALUE;
                }
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
    public abstract SortedBinaryDocValues bytesValues();

    public abstract Bits docsWithValue(int maxDoc);

    public void setNeedsGlobalOrdinals(boolean needsGlobalOrdinals) {}

    public abstract MetaData metaData();

    public static abstract class Bytes extends ValuesSource {

        public Bits docsWithValue(int maxDoc) {
            final SortedBinaryDocValues bytes = bytesValues();
            if (org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(bytes) != null) {
                return org.elasticsearch.index.fielddata.FieldData.unwrapSingletonBits(bytes);
            } else {
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(bytes, maxDoc);
            }
        }

        public static abstract class WithOrdinals extends Bytes implements TopReaderContextAware {

            public Bits docsWithValue(int maxDoc) {
                final RandomAccessOrds ordinals = ordinalsValues();
                if (DocValues.unwrapSingleton(ordinals) != null) {
                    return DocValues.docsWithValue(DocValues.unwrapSingleton(ordinals), maxDoc);
                } else {
                    return DocValues.docsWithValue(ordinals, maxDoc);
                }
            }

            public abstract RandomAccessOrds ordinalsValues();

            public abstract void setNextReader(IndexReaderContext reader);

            public abstract RandomAccessOrds globalOrdinalsValues();

            public abstract long globalMaxOrd(IndexSearcher indexSearcher);

            public static class FieldData extends WithOrdinals implements ReaderContextAware {

                protected final IndexOrdinalsFieldData indexFieldData;
                protected final MetaData metaData;
                private boolean needsGlobalOrdinals;

                protected AtomicOrdinalsFieldData atomicFieldData;
                private SortedBinaryDocValues bytesValues;
                private RandomAccessOrds ordinalsValues;

                protected IndexOrdinalsFieldData globalFieldData;
                protected AtomicOrdinalsFieldData globalAtomicFieldData;
                private RandomAccessOrds globalBytesValues;

                private long maxOrd = -1;

                public FieldData(IndexOrdinalsFieldData indexFieldData, MetaData metaData) {
                    this.indexFieldData = indexFieldData;
                    this.metaData = metaData;
                }

                @Override
                public MetaData metaData() {
                    return metaData;
                }

                @Override
                public void setNeedsGlobalOrdinals(boolean needsGlobalOrdinals) {
                    this.needsGlobalOrdinals = needsGlobalOrdinals;
                }

                @Override
                public void setNextReader(AtomicReaderContext reader) {
                    atomicFieldData = indexFieldData.load(reader);
                    if (bytesValues != null) {
                        bytesValues = atomicFieldData.getBytesValues();
                    }
                    if (ordinalsValues != null) {
                        ordinalsValues = atomicFieldData.getOrdinalsValues();
                    }
                    if (globalFieldData != null) {
                        globalAtomicFieldData = globalFieldData.load(reader);
                        if (globalBytesValues != null) {
                            globalBytesValues = globalAtomicFieldData.getOrdinalsValues();
                        }
                    }
                }

                @Override
                public SortedBinaryDocValues bytesValues() {
                    if (bytesValues == null) {
                        bytesValues = atomicFieldData.getBytesValues();
                    }
                    return bytesValues;
                }

                @Override
                public RandomAccessOrds ordinalsValues() {
                    if (ordinalsValues == null) {
                        ordinalsValues = atomicFieldData.getOrdinalsValues();
                    }
                    return ordinalsValues;
                }

                @Override
                public void setNextReader(IndexReaderContext reader) {
                    if (needsGlobalOrdinals) {
                        globalFieldData = indexFieldData.loadGlobal(reader.reader());
                    }
                }

                @Override
                public RandomAccessOrds globalOrdinalsValues() {
                    if (globalBytesValues == null) {
                        globalBytesValues = globalAtomicFieldData.getOrdinalsValues();
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
                        IndexOrdinalsFieldData globalFieldData = indexFieldData.loadGlobal(indexReader);
                        AtomicOrdinalsFieldData afd = globalFieldData.load(atomicReaderContext);
                        RandomAccessOrds values = afd.getOrdinalsValues();
                        return maxOrd = values.getValueCount();
                    }
                }
            }
        }

        public static class ParentChild extends Bytes implements ReaderContextAware, TopReaderContextAware {

            protected final ParentChildIndexFieldData indexFieldData;
            protected final MetaData metaData;

            protected AtomicParentChildFieldData atomicFieldData;
            protected IndexParentChildFieldData globalFieldData;

            private long maxOrd = -1;

            public ParentChild(ParentChildIndexFieldData indexFieldData, MetaData metaData) {
                this.indexFieldData = indexFieldData;
                this.metaData = metaData;
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                atomicFieldData = globalFieldData.load(reader);
            }

            @Override
            public void setNextReader(IndexReaderContext reader) {
                globalFieldData = indexFieldData.loadGlobal(reader.reader());
            }

            public SortedDocValues globalOrdinalsValues(String type) {
                return atomicFieldData.getOrdinalsValues(type);
            }

            public long globalMaxOrd(IndexSearcher indexSearcher, String type) {
                if (maxOrd != -1) {
                    return maxOrd;
                }

                IndexReader indexReader = indexSearcher.getIndexReader();
                if (indexReader.leaves().isEmpty()) {
                    return maxOrd = 0;
                } else {
                    AtomicReaderContext atomicReaderContext = indexReader.leaves().get(0);
                    IndexParentChildFieldData globalFieldData = indexFieldData.loadGlobal(indexReader);
                    AtomicParentChildFieldData afd = globalFieldData.load(atomicReaderContext);
                    SortedDocValues values = afd.getOrdinalsValues(type);
                    return maxOrd = values.getValueCount();
                }
            }

            @Override
            public SortedBinaryDocValues bytesValues() {
                return atomicFieldData.getBytesValues();
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }
        }

        public static class FieldData extends Bytes implements ReaderContextAware {

            protected final IndexFieldData<?> indexFieldData;
            protected final MetaData metaData;
            protected AtomicFieldData atomicFieldData;
            private SortedBinaryDocValues bytesValues;

            public FieldData(IndexFieldData<?> indexFieldData, MetaData metaData) {
                this.indexFieldData = indexFieldData;
                this.metaData = metaData;
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }

            @Override
            public void setNextReader(AtomicReaderContext reader) {
                atomicFieldData = indexFieldData.load(reader);
                if (bytesValues != null) {
                    bytesValues = atomicFieldData.getBytesValues();
                }
            }

            @Override
            public SortedBinaryDocValues bytesValues() {
                if (bytesValues == null) {
                    bytesValues = atomicFieldData.getBytesValues();
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
            public SortedBinaryDocValues bytesValues() {
                return values;
            }
        }


    }

    public static abstract class Numeric extends ValuesSource {

        /** Whether the underlying data is floating-point or not. */
        public abstract boolean isFloatingPoint();

        /** Get the current {@link LongValues}. */
        public abstract SortedNumericDocValues longValues();

        /** Get the current {@link DoubleValues}. */
        public abstract SortedNumericDoubleValues doubleValues();

        public Bits docsWithValue(int maxDoc) {
            if (isFloatingPoint()) {
                final SortedNumericDoubleValues values = doubleValues();
                if (org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(values) != null) {
                    return org.elasticsearch.index.fielddata.FieldData.unwrapSingletonBits(values);
                } else {
                    return org.elasticsearch.index.fielddata.FieldData.docsWithValue(values, maxDoc);
                }
            } else {
                final SortedNumericDocValues values = longValues();
                if (DocValues.unwrapSingleton(values) != null) {
                    return DocValues.unwrapSingletonBits(values);
                } else {
                    return DocValues.docsWithValue(values, maxDoc);
                }
            }
        }

        public static class WithScript extends Numeric {

            private final SortedNumericDocValues longValues;
            private final SortedNumericDoubleValues doubleValues;
            private final SortedBinaryDocValues bytesValues;

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
            public SortedBinaryDocValues bytesValues() {
                return bytesValues;
            }

            @Override
            public SortedNumericDocValues longValues() {
                return longValues;
            }

            @Override
            public SortedNumericDoubleValues doubleValues() {
                return doubleValues;
            }

            @Override
            public MetaData metaData() {
                return MetaData.UNKNOWN;
            }

            static class LongValues extends SortingNumericDocValues {

                private final Numeric source;
                private final SearchScript script;

                public LongValues(Numeric source, SearchScript script) {
                    this.source = source;
                    this.script = script;
                }

                @Override
                public void setDocument(int docId) {
                    script.setNextDocId(docId);
                    source.longValues().setDocument(docId);
                    count = source.longValues().count();
                    grow();
                    for (int i = 0; i < count; ++i) {
                        script.setNextVar("_value", source.longValues().valueAt(i));
                        values[i] = script.runAsLong();
                    }
                    sort();
                }
            }

            static class DoubleValues extends SortingNumericDoubleValues {

                private final Numeric source;
                private final SearchScript script;

                public DoubleValues(Numeric source, SearchScript script) {
                    this.source = source;
                    this.script = script;
                }

                @Override
                public void setDocument(int docId) {
                    script.setNextDocId(docId);
                    source.doubleValues().setDocument(docId);
                    count = source.doubleValues().count();
                    grow();
                    for (int i = 0; i < count; ++i) {
                        script.setNextVar("_value", source.doubleValues().valueAt(i));
                        values[i] = script.runAsDouble();
                    }
                    sort();
                }
            }
        }

        public static class FieldData extends Numeric implements ReaderContextAware {

            protected boolean needsHashes;
            protected final IndexNumericFieldData indexFieldData;
            protected final MetaData metaData;
            protected AtomicNumericFieldData atomicFieldData;
            private SortedBinaryDocValues bytesValues;
            private SortedNumericDocValues longValues;
            private SortedNumericDoubleValues doubleValues;

            public FieldData(IndexNumericFieldData indexFieldData, MetaData metaData) {
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
            public void setNextReader(AtomicReaderContext reader) {
                atomicFieldData = indexFieldData.load(reader);
                if (bytesValues != null) {
                    bytesValues = atomicFieldData.getBytesValues();
                }
                if (longValues != null) {
                    longValues = atomicFieldData.getLongValues();
                }
                if (doubleValues != null) {
                    doubleValues = atomicFieldData.getDoubleValues();
                }
            }

            @Override
            public SortedBinaryDocValues bytesValues() {
                if (bytesValues == null) {
                    bytesValues = atomicFieldData.getBytesValues();
                }
                return bytesValues;
            }

            @Override
            public SortedNumericDocValues longValues() {
                if (longValues == null) {
                    longValues = atomicFieldData.getLongValues();
                }
                return longValues;
            }

            @Override
            public SortedNumericDoubleValues doubleValues() {
                if (doubleValues == null) {
                    doubleValues = atomicFieldData.getDoubleValues();
                }
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
            public SortedNumericDocValues longValues() {
                return longValues;
            }

            @Override
            public SortedNumericDoubleValues doubleValues() {
                return doubleValues;
            }

            @Override
            public SortedBinaryDocValues bytesValues() {
                return bytesValues;
            }

        }

    }

    // No need to implement ReaderContextAware here, the delegate already takes care of updating data structures
    public static class WithScript extends Bytes {

        private final SortedBinaryDocValues bytesValues;

        public WithScript(ValuesSource delegate, SearchScript script) {
            this.bytesValues = new BytesValues(delegate, script);
        }

        @Override
        public MetaData metaData() {
            return MetaData.UNKNOWN;
        }

        @Override
        public SortedBinaryDocValues bytesValues() {
            return bytesValues;
        }

        static class BytesValues extends SortingBinaryDocValues {

            private final ValuesSource source;
            private final SearchScript script;

            public BytesValues(ValuesSource source, SearchScript script) {
                this.source = source;
                this.script = script;
            }

            @Override
            public void setDocument(int docId) {
                source.bytesValues().setDocument(docId);
                count = source.bytesValues().count();
                grow();
                for (int i = 0; i < count; ++i) {
                    final BytesRef value = source.bytesValues().valueAt(i);
                    script.setNextVar("_value", value.utf8ToString());
                    values[i].copyChars(script.run().toString());
                }
                sort();
            }
        }
    }

    public static class GeoPoint extends ValuesSource implements ReaderContextAware {

        protected final IndexGeoPointFieldData indexFieldData;
        private final MetaData metaData;
        protected AtomicGeoPointFieldData atomicFieldData;
        private SortedBinaryDocValues bytesValues;
        private MultiGeoPointValues geoPointValues;

        public GeoPoint(IndexGeoPointFieldData indexFieldData, MetaData metaData) {
            this.indexFieldData = indexFieldData;
            this.metaData = metaData;
        }

        public Bits docsWithValue(int maxDoc) {
            final MultiGeoPointValues geoPoints = geoPointValues();
            if (org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(geoPoints) != null) {
                return org.elasticsearch.index.fielddata.FieldData.unwrapSingletonBits(geoPoints);
            } else {
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(geoPoints, maxDoc);
            }
        }

        @Override
        public MetaData metaData() {
            return metaData;
        }

        @Override
        public void setNextReader(AtomicReaderContext reader) {
            atomicFieldData = indexFieldData.load(reader);
            if (bytesValues != null) {
                bytesValues = atomicFieldData.getBytesValues();
            }
            if (geoPointValues != null) {
                geoPointValues = atomicFieldData.getGeoPointValues();
            }
        }

        @Override
        public SortedBinaryDocValues bytesValues() {
            if (bytesValues == null) {
                bytesValues = atomicFieldData.getBytesValues();
            }
            return bytesValues;
        }

        public org.elasticsearch.index.fielddata.MultiGeoPointValues geoPointValues() {
            if (geoPointValues == null) {
                geoPointValues = atomicFieldData.getGeoPointValues();
            }
            return geoPointValues;
        }
    }

}
