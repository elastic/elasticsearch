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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.RamAccountingTermsEnum;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * Stores numeric data into bit-packed arrays for better memory efficiency.
 */
public class PackedArrayIndexFieldData extends AbstractIndexFieldData<AtomicNumericFieldData> implements IndexNumericFieldData {

    public static class Builder implements IndexFieldData.Builder {

        private NumericType numericType;

        public Builder setNumericType(NumericType numericType) {
            this.numericType = numericType;
            return this;
        }

        @Override
        public IndexFieldData<AtomicNumericFieldData> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                                            IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
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
        Preconditions.checkArgument(EnumSet.of(NumericType.BOOLEAN, NumericType.BYTE, NumericType.SHORT, NumericType.INT, NumericType.LONG).contains(numericType), getClass().getSimpleName() + " only supports integer types, not " + numericType);
        this.numericType = numericType;
        this.breakerService = breakerService;
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

    @Override
    public AtomicNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        final LeafReader reader = context.reader();
        Terms terms = reader.terms(getFieldNames().indexName());
        AtomicNumericFieldData data = null;
        PackedArrayEstimator estimator = new PackedArrayEstimator(breakerService.getBreaker(CircuitBreaker.FIELDDATA), getNumericType(), getFieldNames().fullName());
        if (terms == null) {
            data = AtomicLongFieldData.empty(reader.maxDoc());
            estimator.adjustForNoTerms(data.ramBytesUsed());
            return data;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        // Lucene encodes numeric data so that the lexicographical (encoded) order matches the integer order so we know the sequence of
        // longs is going to be monotonically increasing
        final PackedLongValues.Builder valuesBuilder = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);

        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
        TermsEnum termsEnum = estimator.beforeLoad(terms);
        assert !getNumericType().isFloatingPoint();
        boolean success = false;
        try (OrdinalsBuilder builder = new OrdinalsBuilder(-1, reader.maxDoc(), acceptableTransientOverheadRatio)) {
            BytesRefIterator iter = builder.buildFromTerms(termsEnum);
            BytesRef term;
            while ((term = iter.next()) != null) {
                final long value = numericType.toLong(term);
                valuesBuilder.add(value);
            }
            final PackedLongValues values = valuesBuilder.build();
            final Ordinals build = builder.build(fieldDataType.getSettings());
            CommonSettings.MemoryStorageFormat formatHint = CommonSettings.getMemoryStorageHint(fieldDataType);

            RandomAccessOrds ordinals = build.ordinals();
            if (FieldData.isMultiValued(ordinals) || formatHint == CommonSettings.MemoryStorageFormat.ORDINALS) {
                final long ramBytesUsed = build.ramBytesUsed() + values.ramBytesUsed();
                data = new AtomicLongFieldData(ramBytesUsed) {

                    @Override
                    public SortedNumericDocValues getLongValues() {
                        return withOrdinals(build, values, reader.maxDoc());
                    }

                    @Override
                    public Collection<Accountable> getChildResources() {
                        List<Accountable> resources = new ArrayList<>();
                        resources.add(Accountables.namedAccountable("ordinals", build));
                        resources.add(Accountables.namedAccountable("values", values));
                        return Collections.unmodifiableList(resources);
                    }
                };
            } else {
                final BitSet docsWithValues = builder.buildDocsWithValuesSet();

                long minV, maxV;
                minV = maxV = 0;
                if (values.size() > 0) {
                    minV = values.get(0);
                    maxV = values.get(values.size() - 1);
                }


                final float acceptableOverheadRatio = fieldDataType.getSettings().getAsFloat("acceptable_overhead_ratio", PackedInts.DEFAULT);
                final int pageSize = fieldDataType.getSettings().getAsInt("single_value_page_size", 1024);

                if (formatHint == null) {
                    formatHint = chooseStorageFormat(reader, values, build, ordinals, minV, maxV, acceptableOverheadRatio, pageSize);
                }

                logger.trace("single value format for field [{}] set to [{}]", getFieldNames().fullName(), formatHint);

                switch (formatHint) {
                    case PACKED:
                        // Encode document without a value with a special value
                        long missingV = 0;
                        if (docsWithValues != null) {
                            if ((maxV - minV + 1) == values.size()) {
                                // values are dense
                                if (minV > Long.MIN_VALUE) {
                                    missingV = --minV;
                                } else {
                                    assert maxV != Long.MAX_VALUE;
                                    missingV = ++maxV;
                                }
                            } else {
                                for (long i = 1; i < values.size(); ++i) {
                                    if (values.get(i) > values.get(i - 1) + 1) {
                                        missingV = values.get(i - 1) + 1;
                                        break;
                                    }
                                }
                            }
                            missingV -= minV;
                        }
                        final long missingValue = missingV;
                        final long minValue = minV;
                        final long maxValue = maxV;

                        final long valuesDelta = maxValue - minValue;
                        int bitsRequired = valuesDelta < 0 ? 64 : PackedInts.bitsRequired(valuesDelta);
                        final PackedInts.Mutable sValues = PackedInts.getMutable(reader.maxDoc(), bitsRequired, acceptableOverheadRatio);

                        if (docsWithValues != null) {
                            sValues.fill(0, sValues.size(), missingV);
                        }

                        for (int i = 0; i < reader.maxDoc(); i++) {
                            ordinals.setDocument(i);
                            if (ordinals.cardinality() > 0) {
                                final long ord = ordinals.ordAt(0);
                                long value = values.get(ord);
                                sValues.set(i, value - minValue);
                            }
                        }
                        long ramBytesUsed = values.ramBytesUsed() + (docsWithValues == null ? 0 : docsWithValues.ramBytesUsed());
                        data = new AtomicLongFieldData(ramBytesUsed) {

                            @Override
                            public SortedNumericDocValues getLongValues() {
                                if (docsWithValues == null) {
                                    return singles(sValues, minValue);
                                } else {
                                    return sparseSingles(sValues, minValue, missingValue, reader.maxDoc());
                                }
                            }
                            
                            @Override
                            public Collection<Accountable> getChildResources() {
                                List<Accountable> resources = new ArrayList<>();
                                resources.add(Accountables.namedAccountable("values", sValues));
                                if (docsWithValues != null) {
                                    resources.add(Accountables.namedAccountable("missing bitset", docsWithValues));
                                }
                                return Collections.unmodifiableList(resources);
                            }

                        };
                        break;
                    case PAGED:
                        final PackedLongValues.Builder dpValues = PackedLongValues.deltaPackedBuilder(pageSize, acceptableOverheadRatio);

                        long lastValue = 0;
                        for (int i = 0; i < reader.maxDoc(); i++) {
                            ordinals.setDocument(i);
                            if (ordinals.cardinality() > 0) {
                                final long ord = ordinals.ordAt(i);
                                lastValue = values.get(ord);
                            }
                            dpValues.add(lastValue);
                        }
                        final PackedLongValues pagedValues = dpValues.build();
                        ramBytesUsed = pagedValues.ramBytesUsed();
                        if (docsWithValues != null) {
                            ramBytesUsed += docsWithValues.ramBytesUsed();
                        }
                        data = new AtomicLongFieldData(ramBytesUsed) {

                            @Override
                            public SortedNumericDocValues getLongValues() {
                                return pagedSingles(pagedValues, docsWithValues);
                            }

                            @Override
                            public Collection<Accountable> getChildResources() {
                                List<Accountable> resources = new ArrayList<>();
                                resources.add(Accountables.namedAccountable("values", pagedValues));
                                if (docsWithValues != null) {
                                    resources.add(Accountables.namedAccountable("missing bitset", docsWithValues));
                                }
                                return Collections.unmodifiableList(resources);
                            }
                            
                        };
                        break;
                    case ORDINALS:
                        ramBytesUsed = build.ramBytesUsed() + values.ramBytesUsed();
                        data = new AtomicLongFieldData(ramBytesUsed) {

                            @Override
                            public SortedNumericDocValues getLongValues() {
                                return withOrdinals(build, values, reader.maxDoc());
                            }
                            
                            @Override
                            public Collection<Accountable> getChildResources() {
                                List<Accountable> resources = new ArrayList<>();
                                resources.add(Accountables.namedAccountable("ordinals", build));
                                resources.add(Accountables.namedAccountable("values", values));
                                return Collections.unmodifiableList(resources);
                            }

                        };
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
                estimator.afterLoad(termsEnum, data.ramBytesUsed());
            }

        }

    }

    protected CommonSettings.MemoryStorageFormat chooseStorageFormat(LeafReader reader, PackedLongValues values, Ordinals build, RandomAccessOrds ordinals,
                                                                     long minValue, long maxValue, float acceptableOverheadRatio, int pageSize) {

        CommonSettings.MemoryStorageFormat format;

        // estimate memory usage for a single packed array
        long packedDelta = maxValue - minValue + 1; // allow for a missing value
        // valuesDelta can be negative if the difference between max and min values overflows the positive side of longs.
        int bitsRequired = packedDelta < 0 ? 64 : PackedInts.bitsRequired(packedDelta);
        PackedInts.FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(reader.maxDoc(), bitsRequired, acceptableOverheadRatio);
        final long singleValuesSize = formatAndBits.format.longCount(PackedInts.VERSION_CURRENT, reader.maxDoc(), formatAndBits.bitsPerValue) * 8L;

        // ordinal memory usage
        final long ordinalsSize = build.ramBytesUsed() + values.ramBytesUsed();

        // estimate the memory signature of paged packing
        long pagedSingleValuesSize = (reader.maxDoc() / pageSize + 1) * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // array of pages
        int pageIndex = 0;
        long pageMinOrdinal = Long.MAX_VALUE;
        long pageMaxOrdinal = Long.MIN_VALUE;
        for (int i = 1; i < reader.maxDoc(); ++i, pageIndex = (pageIndex + 1) % pageSize) {
            ordinals.setDocument(i);
            if (ordinals.cardinality() > 0) {
                long ordinal = ordinals.ordAt(0);
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

    private long getPageMemoryUsage(PackedLongValues values, float acceptableOverheadRatio, int pageSize, long pageMinOrdinal, long pageMaxOrdinal) {
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
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
    }

    /**
     * Estimator that wraps numeric field data loading in a
     * RamAccountingTermsEnum, adjusting the breaker after data has been
     * loaded
     */
    public class PackedArrayEstimator implements PerValueEstimator {

        private final CircuitBreaker breaker;
        private final NumericType type;
        private final String fieldName;

        public PackedArrayEstimator(CircuitBreaker breaker, NumericType type, String fieldName) {
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
            return new RamAccountingTermsEnum(type.wrapTermsEnum(terms.iterator()), breaker, this, this.fieldName);
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

    private static SortedNumericDocValues withOrdinals(Ordinals ordinals, final LongValues values, int maxDoc) {
        final RandomAccessOrds ords = ordinals.ordinals();
        final SortedDocValues singleOrds = DocValues.unwrapSingleton(ords);
        if (singleOrds != null) {
            final NumericDocValues singleValues = new NumericDocValues() {
                @Override
                public long get(int docID) {
                    final int ord = singleOrds.getOrd(docID);
                    if (ord >= 0) {
                        return values.get(singleOrds.getOrd(docID));
                    } else {
                        return 0;
                    }
                }
            };
            return DocValues.singleton(singleValues, DocValues.docsWithValue(ords, maxDoc));
        } else {
            return new SortedNumericDocValues() {
                @Override
                public long valueAt(int index) {
                    return values.get(ords.ordAt(index));
                }

                @Override
                public void setDocument(int doc) {
                    ords.setDocument(doc);
                }

                @Override
                public int count() {
                    return ords.cardinality();
                }
            };
        }
    }

    private static SortedNumericDocValues singles(final NumericDocValues deltas, final long minValue) {
        final NumericDocValues values;
        if (minValue == 0) {
            values = deltas;
        } else {
            values = new NumericDocValues() {
                @Override
                public long get(int docID) {
                    return minValue + deltas.get(docID);
                }
            };
        }
        return DocValues.singleton(values, null);
    }

    private static SortedNumericDocValues sparseSingles(final NumericDocValues deltas, final long minValue,  final long missingValue, final int maxDoc) {
        final NumericDocValues values = new NumericDocValues() {
            @Override
            public long get(int docID) {
                final long delta = deltas.get(docID);
                if (delta == missingValue) {
                    return 0;
                }
                return minValue + delta;
            }
        };
        final Bits docsWithFields = new Bits() {
            @Override
            public boolean get(int index) {
                return deltas.get(index) != missingValue;
            }
            @Override
            public int length() {
                return maxDoc;
            }
        };
        return DocValues.singleton(values, docsWithFields);
    }

    private static SortedNumericDocValues pagedSingles(final PackedLongValues values, final Bits docsWithValue) {
        return DocValues.singleton(new NumericDocValues() {
            // we need to wrap since NumericDocValues must return 0 when a doc has no value
            @Override
            public long get(int docID) {
                if (docsWithValue == null || docsWithValue.get(docID)) {
                    return values.get(docID);
                } else {
                    return 0;
                }
            }
        }, docsWithValue);
    }
}
