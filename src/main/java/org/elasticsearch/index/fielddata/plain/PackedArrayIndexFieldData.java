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
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.*;
import org.apache.lucene.util.packed.AppendingDeltaPackedLongBuffer;
import org.apache.lucene.util.packed.MonotonicAppendingLongBuffer;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;

import java.io.IOException;
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
        public IndexFieldData<AtomicNumericFieldData> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                                            IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService, GlobalOrdinalsBuilder globalOrdinalBuilder) {
            return new PackedArrayIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache, numericType, breakerService);
        }
    }

    private final NumericType numericType;
    private final CircuitBreakerService breakerService;

    public PackedArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames,
                                     FieldDataType fieldDataType, IndexFieldDataCache cache, NumericType numericType,
                                     CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        Preconditions.checkNotNull(numericType);
        Preconditions.checkArgument(EnumSet.of(NumericType.BYTE, NumericType.SHORT, NumericType.INT, NumericType.LONG).contains(numericType), getClass().getSimpleName() + " only supports integer types, not " + numericType);
        this.numericType = numericType;
        this.breakerService = breakerService;
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
        PackedArrayAtomicFieldData data = null;
        PackedArrayEstimator estimator = new PackedArrayEstimator(breakerService.getBreaker(), getNumericType(), getFieldNames().fullName());
        if (terms == null) {
            data = PackedArrayAtomicFieldData.empty();
            estimator.adjustForNoTerms(data.getMemorySizeInBytes());
            return data;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        // Lucene encodes numeric data so that the lexicographical (encoded) order matches the integer order so we know the sequence of
        // longs is going to be monotonically increasing
        final MonotonicAppendingLongBuffer values = new MonotonicAppendingLongBuffer();

        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        TermsEnum termsEnum = estimator.beforeLoad(terms);
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(-1, reader.maxDoc(), acceptableTransientOverheadRatio)) {
            BytesRefIterator iter = builder.buildFromTerms(termsEnum);
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
            CommonSettings.MemoryStorageFormat formatHint = CommonSettings.getMemoryStorageHint(fieldDataType);

            if (build.isMultiValued() || formatHint == CommonSettings.MemoryStorageFormat.ORDINALS) {
                data = new PackedArrayAtomicFieldData.WithOrdinals(values, build);
            } else {
                Docs ordinals = build.ordinals();
                final FixedBitSet docsWithValues = builder.buildDocsWithValuesSet();

                long minValue, maxValue;
                minValue = maxValue = 0;
                if (values.size() > 0) {
                    minValue = values.get(0);
                    maxValue = values.get(values.size() - 1);
                }


                final float acceptableOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_overhead_ratio", PackedInts.DEFAULT);
                final int pageSize = fieldDataType.getSettings().getAsInt("single_value_page_size", 1024);

                if (formatHint == null) {
                    formatHint = chooseStorageFormat(reader, values, build, ordinals, minValue, maxValue, acceptableOverheadRatio, pageSize);
                }

                logger.trace("single value format for field [{}] set to [{}]", getFieldNames().fullName(), formatHint);

                switch (formatHint) {
                    case PACKED:
                        // Encode document without a value with a special value
                        long missingValue = 0;
                        if (docsWithValues != null) {
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
                            missingValue -= minValue;
                        }

                        final long valuesDelta = maxValue - minValue;
                        int bitsRequired = valuesDelta < 0 ? 64 : PackedInts.bitsRequired(valuesDelta);
                        final PackedInts.Mutable sValues = PackedInts.getMutable(reader.maxDoc(), bitsRequired, acceptableOverheadRatio);

                        if (docsWithValues != null) {
                            sValues.fill(0, sValues.size(), missingValue);
                        }

                        for (int i = 0; i < reader.maxDoc(); i++) {
                            final long ord = ordinals.getOrd(i);
                            if (ord != Ordinals.MISSING_ORDINAL) {
                                long value = values.get(ord);
                                sValues.set(i, value - minValue);
                            }
                        }
                        if (docsWithValues == null) {
                            data = new PackedArrayAtomicFieldData.Single(sValues, minValue, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                        } else {
                            data = new PackedArrayAtomicFieldData.SingleSparse(sValues, minValue, missingValue, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                        }
                        break;
                    case PAGED:

                        final AppendingDeltaPackedLongBuffer dpValues = new AppendingDeltaPackedLongBuffer(reader.maxDoc() / pageSize + 1, pageSize, acceptableOverheadRatio);

                        long lastValue = 0;
                        for (int i = 0; i < reader.maxDoc(); i++) {
                            final long ord = ordinals.getOrd(i);
                            if (ord != Ordinals.MISSING_ORDINAL) {
                                lastValue = values.get(ord);
                            }
                            dpValues.add(lastValue);
                        }
                        dpValues.freeze();
                        if (docsWithValues == null) {
                            data = new PackedArrayAtomicFieldData.PagedSingle(dpValues, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                        } else {
                            data = new PackedArrayAtomicFieldData.PagedSingleSparse(dpValues, docsWithValues, ordinals.getMaxOrd() - Ordinals.MIN_ORDINAL);
                        }
                        break;
                    case ORDINALS:
                        data = new PackedArrayAtomicFieldData.WithOrdinals(values, build);
                        break;
                    default:
                        throw new ElasticsearchException("unknown memory format: " + formatHint);
                }

            }

            success = true;
            return data;
        } finally {
            if (!success) {
                // If something went wrong, unwind any current estimations we've made
                estimator.afterLoad(termsEnum, 0);
            } else {
                // Adjust as usual, based on the actual size of the field data
                estimator.afterLoad(termsEnum, data.getMemorySizeInBytes());
            }

        }

    }

    protected CommonSettings.MemoryStorageFormat chooseStorageFormat(AtomicReader reader, MonotonicAppendingLongBuffer values, Ordinals build, Docs ordinals,
                                                                     long minValue, long maxValue, float acceptableOverheadRatio, int pageSize) {

        CommonSettings.MemoryStorageFormat format;

        // estimate memory usage for a single packed array
        long packedDelta = maxValue - minValue + 1; // allow for a missing value
        // valuesDelta can be negative if the difference between max and min values overflows the positive side of longs.
        int bitsRequired = packedDelta < 0 ? 64 : PackedInts.bitsRequired(packedDelta);
        PackedInts.FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(reader.maxDoc(), bitsRequired, acceptableOverheadRatio);
        final long singleValuesSize = formatAndBits.format.longCount(PackedInts.VERSION_CURRENT, reader.maxDoc(), formatAndBits.bitsPerValue) * 8L;

        // ordinal memory usage
        final long ordinalsSize = build.getMemorySizeInBytes() + values.ramBytesUsed();

        // estimate the memory signature of paged packing
        long pagedSingleValuesSize = (reader.maxDoc() / pageSize + 1) * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // array of pages
        int pageIndex = 0;
        long pageMinOrdinal = Long.MAX_VALUE;
        long pageMaxOrdinal = Long.MIN_VALUE;
        for (int i = 1; i < reader.maxDoc(); ++i, pageIndex = (pageIndex + 1) % pageSize) {
            long ordinal = ordinals.getOrd(i);
            if (ordinal != Ordinals.MISSING_ORDINAL) {
                pageMaxOrdinal = Math.max(ordinal, pageMaxOrdinal);
                pageMinOrdinal = Math.min(ordinal, pageMinOrdinal);
            }
            if (pageIndex == pageSize - 1) {
                // end of page, we now know enough to estimate memory usage
                pagedSingleValuesSize += getPageMemoryUsage(values, acceptableOverheadRatio, pageSize, pageMinOrdinal, pageMaxOrdinal);

                pageMinOrdinal = Long.MAX_VALUE;
                pageMaxOrdinal = Long.MIN_VALUE;
            }
        }

        if (pageIndex > 0) {
            // last page estimation
            pageIndex++;
            pagedSingleValuesSize += getPageMemoryUsage(values, acceptableOverheadRatio, pageSize, pageMinOrdinal, pageMaxOrdinal);
        }

        if (ordinalsSize < singleValuesSize) {
            if (ordinalsSize < pagedSingleValuesSize) {
                format = CommonSettings.MemoryStorageFormat.ORDINALS;
            } else {
                format = CommonSettings.MemoryStorageFormat.PAGED;
            }
        } else {
            if (pagedSingleValuesSize < singleValuesSize) {
                format = CommonSettings.MemoryStorageFormat.PAGED;
            } else {
                format = CommonSettings.MemoryStorageFormat.PACKED;
            }
        }
        return format;
    }

    private long getPageMemoryUsage(MonotonicAppendingLongBuffer values, float acceptableOverheadRatio, int pageSize, long pageMinOrdinal, long pageMaxOrdinal) {
        int bitsRequired;
        long pageMemorySize = 0;
        PackedInts.FormatAndBits formatAndBits;
        if (pageMaxOrdinal == Long.MIN_VALUE) {
            // empty page - will use the null reader which just stores size
            pageMemorySize += RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_INT);

        } else {
            long pageMinValue = values.get(pageMinOrdinal);
            long pageMaxValue = values.get(pageMaxOrdinal);
            long pageDelta = pageMaxValue - pageMinValue;
            if (pageDelta != 0) {
                bitsRequired = pageDelta < 0 ? 64 : PackedInts.bitsRequired(pageDelta);
                formatAndBits = PackedInts.fastestFormatAndBits(pageSize, bitsRequired, acceptableOverheadRatio);
                pageMemorySize += formatAndBits.format.longCount(PackedInts.VERSION_CURRENT, pageSize, formatAndBits.bitsPerValue) * RamUsageEstimator.NUM_BYTES_LONG;
                pageMemorySize += RamUsageEstimator.NUM_BYTES_LONG; // min value per page storage
            } else {
                // empty page
                pageMemorySize += RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_INT);
            }
        }
        return pageMemorySize;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode) {
        return new LongValuesComparatorSource(this, missingValue, sortMode);
    }

    /**
     * Estimator that wraps numeric field data loading in a
     * RamAccountingTermsEnum, adjusting the breaker after data has been
     * loaded
     */
    public class PackedArrayEstimator implements PerValueEstimator {

        private final MemoryCircuitBreaker breaker;
        private final NumericType type;
        private final String fieldName;

        public PackedArrayEstimator(MemoryCircuitBreaker breaker, NumericType type, String fieldName) {
            this.breaker = breaker;
            this.type = type;
            this.fieldName = fieldName;
        }

        /**
         * @return number of bytes per term, based on the NumericValue.requiredBits()
         */
        @Override
        public long bytesPerValue(BytesRef term) {
            // Estimate about  about 0.8 (8 / 10) compression ratio for
            // numbers, but at least 4 bytes
            return Math.max(type.requiredBits() / 10, 4);
        }

        /**
         * @return A TermsEnum wrapped in a RamAccountingTermsEnum
         * @throws IOException
         */
        @Override
        public TermsEnum beforeLoad(Terms terms) throws IOException {
            return new RamAccountingTermsEnum(type.wrapTermsEnum(terms.iterator(null)), breaker, this, this.fieldName);
        }

        /**
         * Adjusts the breaker based on the aggregated value from the RamAccountingTermsEnum
         *
         * @param termsEnum  terms that were wrapped and loaded
         * @param actualUsed actual field data memory usage
         */
        @Override
        public void afterLoad(TermsEnum termsEnum, long actualUsed) {
            assert termsEnum instanceof RamAccountingTermsEnum;
            long estimatedBytes = ((RamAccountingTermsEnum) termsEnum).getTotalBytes();
            breaker.addWithoutBreaking(-(estimatedBytes - actualUsed));
        }

        /**
         * Adjust the breaker when no terms were actually loaded, but the field
         * data takes up space regardless. For instance, when ordinals are
         * used.
         *
         * @param actualUsed bytes actually used
         */
        public void adjustForNoTerms(long actualUsed) {
            breaker.addWithoutBreaking(actualUsed);
        }
    }
}
