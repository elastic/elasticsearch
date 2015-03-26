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

package org.elasticsearch.index.fielddata.plain;

import com.google.common.base.Preconditions;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * FieldData backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see FieldInfo.DocValuesType#SORTED_NUMERIC
 */
public class SortedNumericDVIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData {
    private final NumericType numericType;
    
    public SortedNumericDVIndexFieldData(Index index, Names fieldNames, NumericType numericType, FieldDataType fieldDataType) {
        super(index, fieldNames, fieldDataType);
        Preconditions.checkArgument(numericType != null, "numericType must be non-null");
        this.numericType = numericType;
    }

    @Override
    public org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        switch (numericType) {
            case FLOAT:
                return new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
            case DOUBLE: 
                return new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
            default:
                assert !numericType.isFloatingPoint();
                return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
        }
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }
    
    @Override
    public AtomicNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public AtomicNumericFieldData load(LeafReaderContext context) {
        final LeafReader reader = context.reader();
        final String field = fieldNames.indexName();
        
        switch (numericType) {
            case FLOAT:
                return new SortedNumericFloatFieldData(reader, field);
            case DOUBLE:
                return new SortedNumericDoubleFieldData(reader, field);
            default:
                return new SortedNumericLongFieldData(reader, field);
        } 
    }
    
    /**
     * FieldData implementation for integral types.
     * <p>
     * Order of values within a document is consistent with 
     * {@link Long#compareTo(Long)}.
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize 
     * for the case where documents have at most one value. In this case
     * {@link DocValues#unwrapSingleton(SortedNumericDocValues)} will return
     * the underlying single-valued NumericDocValues representation, and 
     * {@link DocValues#unwrapSingletonBits(SortedNumericDocValues)} will return
     * a Bits matching documents that have a real value (as opposed to missing).
     */
    static final class SortedNumericLongFieldData extends AtomicLongFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericLongFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            try {
                return DocValues.getSortedNumeric(reader, field);
            } catch (IOException e) {
                throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
            }
        }
        
        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }
    
    /**
     * FieldData implementation for 32-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {code}
     *   bits ^ (bits >> 31) & 0x7fffffff
     * {code}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize 
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation, and 
     * {@link FieldData#unwrapSingletonBits(SortedNumericDoubleValues)} will return
     * a Bits matching documents that have a real value (as opposed to missing).
     */
    static final class SortedNumericFloatFieldData extends AtomicDoubleFieldData {
        final LeafReader reader;
        final String field;
        
        SortedNumericFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);
                
                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleFloatValues(single), DocValues.unwrapSingletonBits(raw));
                } else {
                    return new MultiFloatValues(raw);
                }
            } catch (IOException e) {
                throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
            }
        }
        
        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }
    
    /** 
     * Wraps a NumericDocValues and exposes a single 32-bit float per document.
     */
    static final class SingleFloatValues extends NumericDoubleValues {
        final NumericDocValues in;
        
        SingleFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double get(int docID) {
            return NumericUtils.sortableIntToFloat((int) in.get(docID));
        }
    }
    
    /** 
     * Wraps a SortedNumericDocValues and exposes multiple 32-bit floats per document.
     */
    static final class MultiFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;
        
        MultiFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }
        
        @Override
        public void setDocument(int doc) {
            in.setDocument(doc);
        }

        @Override
        public double valueAt(int index) {
            return NumericUtils.sortableIntToFloat((int) in.valueAt(index));
        }

        @Override
        public int count() {
            return in.count();
        }
    }
    
    /**
     * FieldData implementation for 64-bit double values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Double#compareTo(Double)}, hence the following reversible
     * transformation is applied at both index and search:
     * {code}
     *   bits ^ (bits >> 63) & 0x7fffffffffffffffL
     * {code}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize 
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation, and 
     * {@link FieldData#unwrapSingletonBits(SortedNumericDoubleValues)} will return
     * a Bits matching documents that have a real value (as opposed to missing).
     */
    static final class SortedNumericDoubleFieldData extends AtomicDoubleFieldData {
        final LeafReader reader;
        final String field;
        
        SortedNumericDoubleFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);
                return FieldData.sortableLongBitsToDoubles(raw);
            } catch (IOException e) {
                throw new ElasticsearchIllegalStateException("Cannot load doc values", e);
            }
        }
        
        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }
}
