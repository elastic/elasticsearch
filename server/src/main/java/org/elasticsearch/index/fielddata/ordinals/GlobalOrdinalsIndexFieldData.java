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
package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 * Concrete implementation of {@link IndexOrdinalsFieldData} for global ordinals.
 * A single instance of this class should be used to cache global ordinals per {@link DirectoryReader}.
 * However {@link #loadGlobal(DirectoryReader)} always creates a new instance of {@link Consumer} from the cached
 * value in order to reuse the segment's {@link TermsEnum} that are needed to retrieve terms from global ordinals.
 * Each instance of {@link Consumer} uses a new set of {@link TermsEnum} that can be reused during the collection,
 * this is done to avoid creating all segment's {@link TermsEnum} each time we want to access the values of a single
 * segment.
 */
public final class GlobalOrdinalsIndexFieldData implements IndexOrdinalsFieldData, Accountable {

    private final String fieldName;
    private final ValuesSourceType valuesSourceType;
    private final long memorySizeInBytes;

    private final OrdinalMap ordinalMap;
    private final LeafOrdinalsFieldData[] segmentAfd;
    private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;

    protected GlobalOrdinalsIndexFieldData(String fieldName,
                                           ValuesSourceType valuesSourceType,
                                           LeafOrdinalsFieldData[] segmentAfd,
                                           OrdinalMap ordinalMap,
                                           long memorySizeInBytes,
                                           Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
        this.memorySizeInBytes = memorySizeInBytes;
        this.ordinalMap = ordinalMap;
        this.segmentAfd = segmentAfd;
        this.scriptFunction = scriptFunction;
    }

    public IndexOrdinalsFieldData newConsumer(DirectoryReader source) {
        return new Consumer(source);
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        throw new IllegalStateException("loadDirect(LeafReaderContext) should not be called in this context");
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
        return this;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new UnsupportedOperationException("no global ordinals sorting yet");
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
            SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public long ramBytesUsed() {
        return memorySizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: break down ram usage?
        return Collections.emptyList();
    }

    @Override
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        throw new IllegalStateException("load(LeafReaderContext) should not be called in this context");
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return ordinalMap;
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return true;
    }

    /**
     * A non-thread safe {@link IndexOrdinalsFieldData} for global ordinals that creates the {@link TermsEnum} of each
     * segment once and use them to provide a single lookup per segment.
     */
    public class Consumer implements IndexOrdinalsFieldData, Accountable {
        private final DirectoryReader source;
        private TermsEnum[] lookups;

        Consumer(DirectoryReader source) {
            this.source = source;
        }

        /**
         * Lazy creation of the {@link TermsEnum} for each segment present in this reader
         */
        private TermsEnum[] getOrLoadTermsEnums() {
            if (lookups == null) {
                lookups = new TermsEnum[segmentAfd.length];
                for (int i = 0; i < lookups.length; i++) {
                    try {
                        lookups[i] = segmentAfd[i].getOrdinalsValues().termsEnum();
                    } catch (IOException e) {
                        throw new UncheckedIOException("Failed to load terms enum", e);
                    }
                }
            }
            return lookups;
        }

        @Override
        public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
            return load(context);
        }

        @Override
        public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
            return this;
        }

        @Override
        public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) throws Exception {
            return this;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public ValuesSourceType getValuesSourceType() {
            return valuesSourceType;
        }

        @Override
        public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
            throw new UnsupportedOperationException("no global ordinals sorting yet");
        }

        @Override
        public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, Nested nested,
                SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
            throw new IllegalArgumentException("only supported on numeric fields");
        }

        @Override
        public long ramBytesUsed() {
            return memorySizeInBytes;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public LeafOrdinalsFieldData load(LeafReaderContext context) {
            assert source.getReaderCacheHelper().getKey() == context.parent.reader().getReaderCacheHelper().getKey();
            return new AbstractLeafOrdinalsFieldData(scriptFunction) {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    final SortedSetDocValues values = segmentAfd[context.ord].getOrdinalsValues();
                    if (values.getValueCount() == ordinalMap.getValueCount()) {
                        // segment ordinals match global ordinals
                        return values;
                    }
                    final TermsEnum[] atomicLookups = getOrLoadTermsEnums();
                    return new GlobalOrdinalMapping(ordinalMap, values, atomicLookups, context.ord);
                }

                @Override
                public long ramBytesUsed() {
                    return segmentAfd[context.ord].ramBytesUsed();
                }


                @Override
                public Collection<Accountable> getChildResources() {
                    return segmentAfd[context.ord].getChildResources();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean supportsGlobalOrdinalsMapping() {
            return true;
        }

        @Override
        public OrdinalMap getOrdinalMap() {
            return ordinalMap;
        }

    }
}
