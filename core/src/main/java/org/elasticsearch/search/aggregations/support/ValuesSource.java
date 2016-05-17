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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.AtomicParentChildFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexParentChildFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingNumericDocValues;
import org.elasticsearch.index.fielddata.SortingNumericDoubleValues;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.support.ValuesSource.WithScript.BytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;

import java.io.IOException;

public abstract class ValuesSource {

    /**
     * Get the current {@link BytesValues}.
     */
    public abstract SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException;

    public abstract Bits docsWithValue(LeafReaderContext context) throws IOException;

    /** Whether this values source needs scores. */
    public boolean needsScores() {
        return false;
    }

    public static abstract class Bytes extends ValuesSource {

        public static final WithOrdinals EMPTY = new WithOrdinals() {

            @Override
            public RandomAccessOrds ordinalsValues(LeafReaderContext context) {
                return DocValues.emptySortedSet();
            }

            @Override
            public RandomAccessOrds globalOrdinalsValues(LeafReaderContext context) {
                return DocValues.emptySortedSet();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary(context.reader().maxDoc());
            }

        };

        @Override
        public Bits docsWithValue(LeafReaderContext context) throws IOException {
            final SortedBinaryDocValues bytes = bytesValues(context);
            if (org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(bytes) != null) {
                return org.elasticsearch.index.fielddata.FieldData.unwrapSingletonBits(bytes);
            } else {
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(bytes, context.reader().maxDoc());
            }
        }

        public static abstract class WithOrdinals extends Bytes {

            @Override
            public Bits docsWithValue(LeafReaderContext context) {
                final RandomAccessOrds ordinals = ordinalsValues(context);
                if (DocValues.unwrapSingleton(ordinals) != null) {
                    return DocValues.docsWithValue(DocValues.unwrapSingleton(ordinals), context.reader().maxDoc());
                } else {
                    return DocValues.docsWithValue(ordinals, context.reader().maxDoc());
                }
            }

            public abstract RandomAccessOrds ordinalsValues(LeafReaderContext context);

            public abstract RandomAccessOrds globalOrdinalsValues(LeafReaderContext context);

            public long globalMaxOrd(IndexSearcher indexSearcher) {
                IndexReader indexReader = indexSearcher.getIndexReader();
                if (indexReader.leaves().isEmpty()) {
                    return 0;
                } else {
                    LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
                    RandomAccessOrds values = globalOrdinalsValues(atomicReaderContext);
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
                    final AtomicOrdinalsFieldData atomicFieldData = indexFieldData.load(context);
                    return atomicFieldData.getBytesValues();
                }

                @Override
                public RandomAccessOrds ordinalsValues(LeafReaderContext context) {
                    final AtomicOrdinalsFieldData atomicFieldData = indexFieldData.load(context);
                    return atomicFieldData.getOrdinalsValues();
                }

                @Override
                public RandomAccessOrds globalOrdinalsValues(LeafReaderContext context) {
                    final IndexOrdinalsFieldData global = indexFieldData.loadGlobal((DirectoryReader)context.parent.reader());
                    final AtomicOrdinalsFieldData atomicFieldData = global.load(context);
                    return atomicFieldData.getOrdinalsValues();
                }
            }
        }

        public static class ParentChild extends Bytes {

            protected final ParentChildIndexFieldData indexFieldData;

            public ParentChild(ParentChildIndexFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            public long globalMaxOrd(IndexSearcher indexSearcher, String type) {
                DirectoryReader indexReader = (DirectoryReader) indexSearcher.getIndexReader();
                if (indexReader.leaves().isEmpty()) {
                    return 0;
                } else {
                    LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
                    IndexParentChildFieldData globalFieldData = indexFieldData.loadGlobal(indexReader);
                    AtomicParentChildFieldData afd = globalFieldData.load(atomicReaderContext);
                    SortedDocValues values = afd.getOrdinalsValues(type);
                    return values.getValueCount();
                }
            }

            public SortedDocValues globalOrdinalsValues(String type, LeafReaderContext context) {
                final IndexParentChildFieldData global = indexFieldData.loadGlobal((DirectoryReader)context.parent.reader());
                final AtomicParentChildFieldData atomicFieldData = global.load(context);
                return atomicFieldData.getOrdinalsValues(type);
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                final AtomicParentChildFieldData atomicFieldData = indexFieldData.load(context);
                return atomicFieldData.getBytesValues();
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

        public static class Script extends Bytes {

            private final SearchScript script;

            public Script(SearchScript script) {
                this.script = script;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.getLeafSearchScript(context));
            }

            @Override
            public boolean needsScores() {
                return script.needsScores();
            }
        }


    }

    public static abstract class Numeric extends ValuesSource {

        public static final Numeric EMPTY = new Numeric() {

            @Override
            public boolean isFloatingPoint() {
                return false;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) {
                return DocValues.emptySortedNumeric(context.reader().maxDoc());
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedNumericDoubles(context.reader().maxDoc());
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary(context.reader().maxDoc());
            }

        };

        /** Whether the underlying data is floating-point or not. */
        public abstract boolean isFloatingPoint();

        /** Get the current {@link SortedNumericDocValues}. */
        public abstract SortedNumericDocValues longValues(LeafReaderContext context) throws IOException;

        /** Get the current {@link SortedNumericDoubleValues}. */
        public abstract SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException;

        @Override
        public Bits docsWithValue(LeafReaderContext context) throws IOException {
            if (isFloatingPoint()) {
                final SortedNumericDoubleValues values = doubleValues(context);
                if (org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(values) != null) {
                    return org.elasticsearch.index.fielddata.FieldData.unwrapSingletonBits(values);
                } else {
                    return org.elasticsearch.index.fielddata.FieldData.docsWithValue(values, context.reader().maxDoc());
                }
            } else {
                final SortedNumericDocValues values = longValues(context);
                if (DocValues.unwrapSingleton(values) != null) {
                    return DocValues.unwrapSingletonBits(values);
                } else {
                    return DocValues.docsWithValue(values, context.reader().maxDoc());
                }
            }
        }

        public static class WithScript extends Numeric {

            private final Numeric delegate;
            private final SearchScript script;

            public WithScript(Numeric delegate, SearchScript script) {
                this.delegate = delegate;
                this.script = script;
            }

            @Override
            public boolean isFloatingPoint() {
                return true; // even if the underlying source produces longs, scripts can change them to doubles
            }

            @Override
            public boolean needsScores() {
                return script.needsScores();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ValuesSource.WithScript.BytesValues(delegate.bytesValues(context), script.getLeafSearchScript(context));
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new LongValues(delegate.longValues(context), script.getLeafSearchScript(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new DoubleValues(delegate.doubleValues(context), script.getLeafSearchScript(context));
            }

            static class LongValues extends SortingNumericDocValues implements ScorerAware {

                private final SortedNumericDocValues longValues;
                private final LeafSearchScript script;

                public LongValues(SortedNumericDocValues values, LeafSearchScript script) {
                    this.longValues = values;
                    this.script = script;
                }

                @Override
                public void setDocument(int doc) {
                    longValues.setDocument(doc);
                    resize(longValues.count());
                    script.setDocument(doc);
                    for (int i = 0; i < count(); ++i) {
                        script.setNextAggregationValue(longValues.valueAt(i));
                        values[i] = script.runAsLong();
                    }
                    sort();
                }

                @Override
                public void setScorer(Scorer scorer) {
                    script.setScorer(scorer);
                }
            }

            static class DoubleValues extends SortingNumericDoubleValues implements ScorerAware {

                private final SortedNumericDoubleValues doubleValues;
                private final LeafSearchScript script;

                public DoubleValues(SortedNumericDoubleValues values, LeafSearchScript script) {
                    this.doubleValues = values;
                    this.script = script;
                }

                @Override
                public void setDocument(int doc) {
                    doubleValues.setDocument(doc);
                    resize(doubleValues.count());
                    script.setDocument(doc);
                    for (int i = 0; i < count(); ++i) {
                        script.setNextAggregationValue(doubleValues.valueAt(i));
                        values[i] = script.runAsDouble();
                    }
                    sort();
                }

                @Override
                public void setScorer(Scorer scorer) {
                    script.setScorer(scorer);
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

        public static class Script extends Numeric {
            private final SearchScript script;
            private final ValueType scriptValueType;

            public Script(SearchScript script, ValueType scriptValueType) {
                this.script = script;
                this.scriptValueType = scriptValueType;
            }

            @Override
            public boolean isFloatingPoint() {
                return scriptValueType != null ? scriptValueType.isFloatingPoint() : true;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new ScriptLongValues(script.getLeafSearchScript(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new ScriptDoubleValues(script.getLeafSearchScript(context));
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.getLeafSearchScript(context));
            }

            @Override
            public boolean needsScores() {
                return script.needsScores();
            }
        }

    }

    // No need to implement ReaderContextAware here, the delegate already takes care of updating data structures
    public static class WithScript extends Bytes {

        private final ValuesSource delegate;
        private final SearchScript script;

        public WithScript(ValuesSource delegate, SearchScript script) {
            this.delegate = delegate;
            this.script = script;
        }

        @Override
        public boolean needsScores() {
            return script.needsScores();
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
            return new BytesValues(delegate.bytesValues(context), script.getLeafSearchScript(context));
        }

        static class BytesValues extends SortingBinaryDocValues implements ScorerAware {

            private final SortedBinaryDocValues bytesValues;
            private final LeafSearchScript script;

            public BytesValues(SortedBinaryDocValues bytesValues, LeafSearchScript script) {
                this.bytesValues = bytesValues;
                this.script = script;
            }

            @Override
            public void setDocument(int docId) {
                bytesValues.setDocument(docId);
                count = bytesValues.count();
                grow();
                for (int i = 0; i < count; ++i) {
                    final BytesRef value = bytesValues.valueAt(i);
                    script.setNextAggregationValue(value.utf8ToString());
                    values[i].copyChars(script.run().toString());
                }
                sort();
            }

            @Override
            public void setScorer(Scorer scorer) {
                script.setScorer(scorer);
            }
        }
    }

    public static abstract class GeoPoint extends ValuesSource {

        public static final GeoPoint EMPTY = new GeoPoint() {

            @Override
            public MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return org.elasticsearch.index.fielddata.FieldData.emptyMultiGeoPoints(context.reader().maxDoc());
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary(context.reader().maxDoc());
            }

        };

        @Override
        public Bits docsWithValue(LeafReaderContext context) {
            final MultiGeoPointValues geoPoints = geoPointValues(context);
            if (org.elasticsearch.index.fielddata.FieldData.unwrapSingleton(geoPoints) != null) {
                return org.elasticsearch.index.fielddata.FieldData.unwrapSingletonBits(geoPoints);
            } else {
                return org.elasticsearch.index.fielddata.FieldData.docsWithValue(geoPoints, context.reader().maxDoc());
            }
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
