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

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.lucene.index.*;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * ParentChildIndexFieldData is responsible for loading the id cache mapping
 * needed for has_child and has_parent queries into memory.
 */
public class ParentChildIndexFieldData extends AbstractIndexFieldData<AtomicParentChildFieldData> implements IndexParentChildFieldData, DocumentTypeListener {

    private final NavigableSet<BytesRef> parentTypes;
    private final CircuitBreakerService breakerService;

    // If child type (a type with _parent field) is added or removed, we want to make sure modifications don't happen
    // while loading.
    private final Object lock = new Object();

    public ParentChildIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames,
                                     FieldDataType fieldDataType, IndexFieldDataCache cache, MapperService mapperService,
                                     CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        parentTypes = new TreeSet<>(BytesRef.getUTF8SortedAsUnicodeComparator());
        this.breakerService = breakerService;
        for (DocumentMapper documentMapper : mapperService.docMappers(false)) {
            beforeCreate(documentMapper);
        }
        mapperService.addTypeListener(this);
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    public ParentChildAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat(
                "acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO
        );

        final NavigableSet<BytesRef> parentTypes;
        synchronized (lock) {
            parentTypes = ImmutableSortedSet.copyOf(BytesRef.getUTF8SortedAsUnicodeComparator(), this.parentTypes);
        }
        boolean success = false;
        ParentChildAtomicFieldData data = null;
        ParentChildFilteredTermsEnum termsEnum = new ParentChildFilteredTermsEnum(
                new ParentChildIntersectTermsEnum(reader, UidFieldMapper.NAME, ParentFieldMapper.NAME),
                parentTypes
        );
        ParentChildEstimator estimator = new ParentChildEstimator(breakerService.getBreaker(CircuitBreaker.Name.FIELDDATA), termsEnum);
        TermsEnum estimatedTermsEnum = estimator.beforeLoad(null);
        ObjectObjectOpenHashMap<String, TypeBuilder> typeBuilders = ObjectObjectOpenHashMap.newInstance();
        try {
            try {
                DocsEnum docsEnum = null;
                for (BytesRef term = estimatedTermsEnum.next(); term != null; term = estimatedTermsEnum.next()) {
                    // Usually this would be estimatedTermsEnum, but the
                    // abstract TermsEnum class does not support the .type()
                    // and .id() methods, so we skip using the wrapped
                    // TermsEnum and delegate directly to the
                    // ParentChildFilteredTermsEnum that was originally wrapped
                    String type = termsEnum.type();
                    TypeBuilder typeBuilder = typeBuilders.get(type);
                    if (typeBuilder == null) {
                        typeBuilders.put(type, typeBuilder = new TypeBuilder(acceptableTransientOverheadRatio, reader));
                    }

                    BytesRef id = termsEnum.id();
                    final long termOrd = typeBuilder.builder.nextOrdinal();
                    assert termOrd == typeBuilder.termOrdToBytesOffset.size();
                    typeBuilder.termOrdToBytesOffset.add(typeBuilder.bytes.copyUsingLengthPrefix(id));
                    docsEnum = estimatedTermsEnum.docs(null, docsEnum, DocsEnum.FLAG_NONE);
                    for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                        typeBuilder.builder.addDoc(docId);
                    }
                }

                ImmutableOpenMap.Builder<String, AtomicOrdinalsFieldData> typeToAtomicFieldData = ImmutableOpenMap.builder(typeBuilders.size());
                for (ObjectObjectCursor<String, TypeBuilder> cursor : typeBuilders) {
                    PagedBytes.Reader bytesReader = cursor.value.bytes.freeze(true);
                    final Ordinals ordinals = cursor.value.builder.build(fieldDataType.getSettings());

                    typeToAtomicFieldData.put(
                            cursor.key,
                            new PagedBytesAtomicFieldData(bytesReader, cursor.value.termOrdToBytesOffset.build(), ordinals)
                    );
                }
                data = new ParentChildAtomicFieldData(typeToAtomicFieldData.build());
            } finally {
                for (ObjectObjectCursor<String, TypeBuilder> cursor : typeBuilders) {
                    cursor.value.builder.close();
                }
            }
            success = true;
            return data;
        } finally {
            if (success) {
                estimator.afterLoad(estimatedTermsEnum, data.ramBytesUsed());
            } else {
                estimator.afterLoad(estimatedTermsEnum, 0);
            }
        }
    }

    @Override
    public void beforeCreate(DocumentMapper mapper) {
        synchronized (lock) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                // A _parent field can never be added to an existing mapping, so a _parent field either exists on
                // a new created or doesn't exists. This is why we can update the known parent types via DocumentTypeListener
                if (parentTypes.add(new BytesRef(parentFieldMapper.type()))) {
                    clear();
                }
            }
        }
    }

    @Override
    public void afterRemove(DocumentMapper mapper) {
        synchronized (lock) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                parentTypes.remove(new BytesRef(parentFieldMapper.type()));
            }
        }
    }

    class TypeBuilder {

        final PagedBytes bytes;
        final PackedLongValues.Builder termOrdToBytesOffset;
        final OrdinalsBuilder builder;

        TypeBuilder(float acceptableTransientOverheadRatio, AtomicReader reader) throws IOException {
            bytes = new PagedBytes(15);
            termOrdToBytesOffset = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            builder = new OrdinalsBuilder(-1, reader.maxDoc(), acceptableTransientOverheadRatio);
        }
    }

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, FieldMapper<?> mapper,
                                       IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       MapperService mapperService) {
            return new ParentChildIndexFieldData(index, indexSettings, mapper.names(), mapper.fieldDataType(), cache,
                    mapperService, breakerService);
        }
    }

    /**
     * Estimator that wraps parent/child id field data by wrapping the data
     * in a RamAccountingTermsEnum.
     */
    public class ParentChildEstimator implements PerValueEstimator {

        private final CircuitBreaker breaker;
        private final TermsEnum filteredEnum;

        // The TermsEnum is passed in here instead of being generated in the
        // beforeLoad() function since it's filtered inside the previous
        // TermsEnum wrappers
        public ParentChildEstimator(CircuitBreaker breaker, TermsEnum filteredEnum) {
            this.breaker = breaker;
            this.filteredEnum = filteredEnum;
        }

        /**
         * General overhead for ids is 2 times the length of the ID
         */
        @Override
        public long bytesPerValue(BytesRef term) {
            if (term == null) {
                return 0;
            }
            return 2 * term.length;
        }

        /**
         * Wraps the already filtered {@link TermsEnum} in a
         * {@link RamAccountingTermsEnum} and returns it
         */
        @Override
        public TermsEnum beforeLoad(Terms terms) throws IOException {
            return new RamAccountingTermsEnum(filteredEnum, breaker, this, "parent/child id cache");
        }

        /**
         * Adjusts the breaker based on the difference between the actual usage
         * and the aggregated estimations.
         */
        @Override
        public void afterLoad(TermsEnum termsEnum, long actualUsed) {
            assert termsEnum instanceof RamAccountingTermsEnum;
            long estimatedBytes = ((RamAccountingTermsEnum) termsEnum).getTotalBytes();
            breaker.addWithoutBreaking(-(estimatedBytes - actualUsed));
        }
    }

    @Override
    public IndexParentChildFieldData loadGlobal(IndexReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        try {
            return cache.load(indexReader, this);
        } catch (Throwable e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public IndexParentChildFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
        final long startTime = System.nanoTime();
        final Map<String, SortedDocValues[]> types = new HashMap<>();
        synchronized (lock) {
            for (BytesRef type : parentTypes) {
                final SortedDocValues[] values = new SortedDocValues[indexReader.leaves().size()];
                Arrays.fill(values, DocValues.emptySorted());
                types.put(type.utf8ToString(), values);
            }
        }

        for (Map.Entry<String, SortedDocValues[]> entry : types.entrySet()) {
            final String parentType = entry.getKey();
            final SortedDocValues[] values = entry.getValue();
            for (AtomicReaderContext context : indexReader.leaves()) {
                SortedDocValues vals = load(context).getOrdinalsValues(parentType);
                if (vals != null) {
                    values[context.ord] = vals;
                }
            }
        }

        long ramBytesUsed = 0;
        @SuppressWarnings("unchecked")
        final Map<String, SortedDocValues>[] global = new Map[indexReader.leaves().size()];
        for (Map.Entry<String, SortedDocValues[]> entry : types.entrySet()) {
            final String parentType = entry.getKey();
            final SortedDocValues[] values = entry.getValue();
            final OrdinalMap ordinalMap = OrdinalMap.build(null, entry.getValue(), PackedInts.DEFAULT);
            ramBytesUsed += ordinalMap.ramBytesUsed();
            for (int i = 0; i < values.length; ++i) {
                final SortedDocValues segmentValues = values[i];
                final LongValues globalOrds = ordinalMap.getGlobalOrds(i);
                final SortedDocValues globalSortedValues = new SortedDocValues() {
                    @Override
                    public BytesRef lookupOrd(int ord) {
                        final int segmentNum = ordinalMap.getFirstSegmentNumber(ord);
                        final int segmentOrd = (int) ordinalMap.getFirstSegmentOrd(ord);
                        return values[segmentNum].lookupOrd(segmentOrd);
                    }

                    @Override
                    public int getValueCount() {
                        return (int) ordinalMap.getValueCount();
                    }

                    @Override
                    public int getOrd(int docID) {
                        final int segmentOrd = segmentValues.getOrd(docID);
                        // TODO: is there a way we can get rid of this branch?
                        if (segmentOrd >= 0) {
                            return (int) globalOrds.get(segmentOrd);
                        } else {
                            return segmentOrd;
                        }
                    }
                };
                Map<String, SortedDocValues> perSegmentGlobal = global[i];
                if (perSegmentGlobal == null) {
                    perSegmentGlobal = new HashMap<>(1);
                    global[i] = perSegmentGlobal;
                }
                perSegmentGlobal.put(parentType, globalSortedValues);
            }
        }

        breakerService.getBreaker(CircuitBreaker.Name.FIELDDATA).addWithoutBreaking(ramBytesUsed);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Global-ordinals[_parent] took {}",
                    new TimeValue(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)
            );
        }

        return new GlobalFieldData(indexReader, global, ramBytesUsed);
    }

    private class GlobalFieldData implements IndexParentChildFieldData, Accountable {

        private final AtomicParentChildFieldData[] atomicFDs;
        private final IndexReader reader;
        private final long ramBytesUsed;

        GlobalFieldData(IndexReader reader, final Map<String, SortedDocValues>[] globalValues, long ramBytesUsed) {
            this.reader = reader;
            this.ramBytesUsed = ramBytesUsed;
            this.atomicFDs = new AtomicParentChildFieldData[globalValues.length];
            for (int i = 0; i < globalValues.length; ++i) {
                final int ord = i;
                atomicFDs[i] = new AbstractAtomicParentChildFieldData() {
                    @Override
                    public long ramBytesUsed() {
                        return 0;
                    }

                    @Override
                    public void close() {
                    }

                    @Override
                    public Set<String> types() {
                        return Collections.unmodifiableSet(globalValues[ord].keySet());
                    }

                    @Override
                    public SortedDocValues getOrdinalsValues(String type) {
                        SortedDocValues dv = globalValues[ord].get(type);
                        if (dv == null) {
                            dv = DocValues.emptySorted();
                        }
                        return dv;
                    }
                };
            }
        }

        @Override
        public Names getFieldNames() {
            return ParentChildIndexFieldData.this.getFieldNames();
        }

        @Override
        public FieldDataType getFieldDataType() {
            return ParentChildIndexFieldData.this.getFieldDataType();
        }

        @Override
        public AtomicParentChildFieldData load(AtomicReaderContext context) {
            assert context.reader().getCoreCacheKey() == reader.leaves().get(context.ord).reader().getCoreCacheKey();
            return atomicFDs[context.ord];
        }

        @Override
        public AtomicParentChildFieldData loadDirect(AtomicReaderContext context) throws Exception {
            return load(context);
        }

        @Override
        public XFieldComparatorSource comparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
            throw new UnsupportedOperationException("No sorting on global ords");
        }

        @Override
        public void clear() {
            ParentChildIndexFieldData.this.clear();
        }

        @Override
        public void clear(IndexReader reader) {
            ParentChildIndexFieldData.this.clear(reader);
        }

        @Override
        public Index index() {
            return ParentChildIndexFieldData.this.index();
        }

        @Override
        public long ramBytesUsed() {
            return ramBytesUsed;
        }

        @Override
        public IndexParentChildFieldData loadGlobal(IndexReader indexReader) {
            if (indexReader.getCoreCacheKey() == reader.getCoreCacheKey()) {
                return this;
            }
            throw new ElasticsearchIllegalStateException();
        }

        @Override
        public IndexParentChildFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
            return loadGlobal(indexReader);
        }

    }

}
