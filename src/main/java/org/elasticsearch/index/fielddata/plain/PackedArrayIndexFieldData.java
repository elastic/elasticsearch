/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.EnumSet;

/**
 * Stores numeric data into bit-packed arrays for better memory efficiency.
 */
public class PackedArrayIndexFieldData extends AbstractIndexFieldData<AtomicNumericFieldData> implements IndexNumericFieldData<AtomicNumericFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        private NumericType numericType;

        public Builder setNumericType(NumericType numericType) {
            this.numericType = numericType;
            return this;
        }

        @Override
        public IndexFieldData<AtomicNumericFieldData> build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new PackedArrayIndexFieldData(index, indexSettings, fieldNames, type, cache, numericType);
        }
    }

    private final NumericType numericType;

    public PackedArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache, NumericType numericType) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        Preconditions.checkNotNull(numericType);
        Preconditions.checkArgument(EnumSet.of(NumericType.BYTE, NumericType.SHORT, NumericType.INT, NumericType.LONG).contains(numericType), getClass().getSimpleName() + " only supports integer types, not " + numericType);
        this.numericType = numericType;
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

    @Override
    public boolean valuesOrdered() {
        // because we might have single values? we can dynamically update a flag to reflect that
        // based on the atomic field data loaded
        return false;
    }

    @Override
    public AtomicNumericFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();
        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return PackedArrayAtomicFieldData.empty(reader.maxDoc());
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        // Lucene encodes numeric data so that the lexicographical (encoded) order matches the integer order so we know the sequence of
        // longs is going to be monotonically increasing
        final MonotonicAppendingLongBuffer values = new MonotonicAppendingLongBuffer();

        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        OrdinalsBuilder builder = new OrdinalsBuilder(-1, reader.maxDoc(), acceptableTransientOverheadRatio);
        try {
            BytesRefIterator iter = builder.buildFromTerms(getNumericType().wrapTermsEnum(terms.iterator(null)));
            BytesRef term;
            assert !getNumericType().isFloatingPoint();
            final boolean indexedAsLong = getNumericType().requiredBits() > 32;
            while ((term = iter.next()) != null) {
                final long value = indexedAsLong
                        ? NumericUtils.prefixCodedToLong(term)
                        : NumericUtils.prefixCodedToInt(term);
                assert values.size() == 0 || value > values.get(values.size() - 1);
                values.add(value);
            }
            Ordinals build = builder.build(fieldDataType.getSettings());

            if (!build.isMultiValued() && CommonSettings.removeOrdsOnSingleValue(fieldDataType)) {
                Docs ordinals = build.ordinals();
                final FixedBitSet set = builder.buildDocsWithValuesSet();

                long minValue, maxValue;
                minValue = maxValue = 0;
                if (values.size() > 0) {
                    minValue = values.get(0);
                    maxValue = values.get(values.size() - 1);
                }

                // Encode document without a value with a special value
                long missingValue = 0;
                if (set != null) {
                    if ((maxValue - minValue + 1) == values.size()) {
                        // values are dense
                        if (minValue > Long.MIN_VALUE) {
                            missingValue = --minValue;
                        } else {
                            assert maxValue != Long.MAX_VALUE;
                            missingValue = ++maxValue;
                        }
                    } else {
                        for (long i = 1; i < values.size(); ++i) {
                            if (values.get(i) > values.get(i - 1) + 1) {
                                missingValue = values.get(i - 1) + 1;
                                break;
                            }
                        }
                    }
                    missingValue -= minValue; // delta
                }

                final long delta = maxValue - minValue;
                final int bitsRequired = delta < 0 ? 64 : PackedInts.bitsRequired(delta);
                final float acceptableOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_overhead_ratio", PackedInts.DEFAULT);
                final PackedInts.FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(reader.maxDoc(), bitsRequired, acceptableOverheadRatio);

                // there's sweet spot where due to low unique value count, using ordinals will consume less memory
                final long singleValuesSize = formatAndBits.format.longCount(PackedInts.VERSION_CURRENT, reader.maxDoc(), formatAndBits.bitsPerValue) * 8L;
                final long uniqueValuesSize = values.ramBytesUsed();
                final long ordinalsSize = build.getMemorySizeInBytes();

                if (uniqueValuesSize + ordinalsSize < singleValuesSize) {
                    return new PackedArrayAtomicFieldData.WithOrdinals(values, reader.maxDoc(), build);
                }

                final PackedInts.Mutable sValues = PackedInts.getMutable(reader.maxDoc(), bitsRequired, acceptableOverheadRatio);
                if (missingValue != 0) {
                    sValues.fill(0, sValues.size(), missingValue);
                }
                for (int i = 0; i < reader.maxDoc(); i++) {
                    final long ord = ordinals.getOrd(i);
                    if (ord != Ordinals.MISSING_ORDINAL) {
                        sValues.set(i, values.get(ord - 1) - minValue);
                    }
                }
                if (set == null) {
                    return new PackedArrayAtomicFieldData.Single(sValues, minValue, reader.maxDoc(), ordinals.getNumOrds());
                } else {
                    return new PackedArrayAtomicFieldData.SingleSparse(sValues, minValue, reader.maxDoc(), missingValue, ordinals.getNumOrds());
                }
            } else {
                return new PackedArrayAtomicFieldData.WithOrdinals(values, reader.maxDoc(), build);
            }
        } finally {
            builder.close();
        }

    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return new LongValuesComparatorSource(this, missingValue, sortMode);
    }
}
