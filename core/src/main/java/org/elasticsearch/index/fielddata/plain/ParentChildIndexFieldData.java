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

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;

import org.apache.lucene.index.*;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentTypeListener;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
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

    private final NavigableSet<String> parentTypes;
    private final CircuitBreakerService breakerService;

    // If child type (a type with _parent field) is added or removed, we want to make sure modifications don't happen
    // while loading.
    private final Object lock = new Object();

    public ParentChildIndexFieldData(Index index, Settings indexSettings, MappedFieldType.Names fieldNames,
                                     FieldDataType fieldDataType, IndexFieldDataCache cache, MapperService mapperService,
                                     CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.breakerService = breakerService;
        if (Version.indexCreated(indexSettings).before(Version.V_2_0_0_beta1)) {
            parentTypes = new TreeSet<>();
            for (DocumentMapper documentMapper : mapperService.docMappers(false)) {
                beforeCreate(documentMapper);
            }
            mapperService.addTypeListener(this);
        } else {
            ImmutableSortedSet.Builder<String> builder = ImmutableSortedSet.naturalOrder();
            for (DocumentMapper mapper : mapperService.docMappers(false)) {
                ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
                if (parentFieldMapper.active()) {
                    builder.add(parentFieldMapper.type());
                }
            }
            parentTypes = builder.build();
        }
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    public AtomicParentChildFieldData load(LeafReaderContext context) {
        if (Version.indexCreated(indexSettings()).onOrAfter(Version.V_2_0_0_beta1)) {
            final LeafReader reader = context.reader();
            return new AbstractAtomicParentChildFieldData() {

                public Set<String> types() {
                    return parentTypes;
                }

                @Override
                public SortedDocValues getOrdinalsValues(String type) {
                    try {
                        return DocValues.getSorted(reader, ParentFieldMapper.joinField(type));
                    } catch (IOException e) {
                        throw new IllegalStateException("cannot load join doc values field for type [" + type + "]", e);
                    }
                }

                @Override
                public long ramBytesUsed() {
                    // unknown
                    return 0;
                }

                @Override
                public Collection<Accountable> getChildResources() {
                    return Collections.emptyList();
                }

                @Override
                public void close() throws ElasticsearchException {
                }
            };
        } else {
            try {
                return cache.load(context, this);
            } catch (Throwable e) {
                if (e instanceof ElasticsearchException) {
                    throw (ElasticsearchException) e;
                } else {
                    throw new ElasticsearchException(e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public AbstractAtomicParentChildFieldData loadDirect(LeafReaderContext context) throws Exception {
        // Make this method throw an UnsupportedOperationException in 3.0, only
        // needed for indices created BEFORE 2.0
        LeafReader reader = context.reader();
        final float acceptableTransientOverheadRatio = fieldDataType.getSettings().getAsFloat(
                "acceptable_transient_overhead_ratio", OrdinalsBuilder.DEFAULT_ACCEPTABLE_OVERHEAD_RATIO
        );

        final NavigableSet<BytesRef> parentTypes = new TreeSet<>();
        synchronized (lock) {
            for (String parentType : this.parentTypes) {
                parentTypes.add(new BytesRef(parentType));
            }
        }
        boolean success = false;
        ParentChildAtomicFieldData data = null;
        ParentChildFilteredTermsEnum termsEnum = new ParentChildFilteredTermsEnum(
                new ParentChildIntersectTermsEnum(reader, UidFieldMapper.NAME, ParentFieldMapper.NAME),
                parentTypes
        );
        ParentChildEstimator estimator = new ParentChildEstimator(breakerService.getBreaker(CircuitBreaker.FIELDDATA), termsEnum);
        TermsEnum estimatedTermsEnum = estimator.beforeLoad(null);
        ObjectObjectHashMap<String, TypeBuilder> typeBuilders = new ObjectObjectHashMap<>();
        try {
            try {
                PostingsEnum docsEnum = null;
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
                    docsEnum = estimatedTermsEnum.postings(docsEnum, PostingsEnum.NONE);
                    for (int docId = docsEnum.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
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
        // Remove in 3.0
        synchronized (lock) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                // A _parent field can never be added to an existing mapping, so a _parent field either exists on
                // a new created or doesn't exists. This is why we can update the known parent types via DocumentTypeListener
                if (parentTypes.add(parentFieldMapper.type())) {
                    clear();
                }
            }
        }
    }

    @Override
    protected AtomicParentChildFieldData empty(int maxDoc) {
        return new ParentChildAtomicFieldData(ImmutableOpenMap.<String, AtomicOrdinalsFieldData>of());
    }

    class TypeBuilder {

        final PagedBytes bytes;
        final PackedLongValues.Builder termOrdToBytesOffset;
        final OrdinalsBuilder builder;

        TypeBuilder(float acceptableTransientOverheadRatio, LeafReader reader) throws IOException {
            bytes = new PagedBytes(15);
            termOrdToBytesOffset = PackedLongValues.monotonicBuilder(PackedInts.COMPACT);
            builder = new OrdinalsBuilder(-1, reader.maxDoc(), acceptableTransientOverheadRatio);
        }
    }

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, Settings indexSettings, MappedFieldType fieldType,
                                       IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       MapperService mapperService) {
            return new ParentChildIndexFieldData(index, indexSettings, fieldType.names(), fieldType.fieldDataType(), cache,
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
    public IndexParentChildFieldData loadGlobal(DirectoryReader indexReader) {
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

    private static OrdinalMap buildOrdinalMap(AtomicParentChildFieldData[] atomicFD, String parentType) throws IOException {
        final SortedDocValues[] ordinals = new SortedDocValues[atomicFD.length];
        for (int i = 0; i < ordinals.length; ++i) {
            ordinals[i] = atomicFD[i].getOrdinalsValues(parentType);
        }
        return OrdinalMap.build(null, ordinals, PackedInts.DEFAULT);
    }

    private static class OrdinalMapAndAtomicFieldData {
        final OrdinalMap ordMap;
        final AtomicParentChildFieldData[] fieldData;

        public OrdinalMapAndAtomicFieldData(OrdinalMap ordMap, AtomicParentChildFieldData[] fieldData) {
            this.ordMap = ordMap;
            this.fieldData = fieldData;
        }
    }

    @Override
    public IndexParentChildFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        final long startTime = System.nanoTime();
        final Set<String> parentTypes;
        if (Version.indexCreated(indexSettings()).before(Version.V_2_0_0_beta1)) {
            synchronized (lock) {
                parentTypes = ImmutableSet.copyOf(this.parentTypes);
            }
        } else {
            parentTypes = this.parentTypes;
        }

        long ramBytesUsed = 0;
        final Map<String, OrdinalMapAndAtomicFieldData> perType = new HashMap<>();
        for (String type : parentTypes) {
            final AtomicParentChildFieldData[] fieldData = new AtomicParentChildFieldData[indexReader.leaves().size()];
            for (LeafReaderContext context : indexReader.leaves()) {
                fieldData[context.ord] = load(context);
            }
            final OrdinalMap ordMap = buildOrdinalMap(fieldData, type);
            ramBytesUsed += ordMap.ramBytesUsed();
            perType.put(type, new OrdinalMapAndAtomicFieldData(ordMap, fieldData));
        }

        final AtomicParentChildFieldData[] fielddata = new AtomicParentChildFieldData[indexReader.leaves().size()];
        for (int i = 0; i < fielddata.length; ++i) {
            fielddata[i] = new GlobalAtomicFieldData(parentTypes, perType, i);
        }

        breakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(ramBytesUsed);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Global-ordinals[_parent] took {}",
                    new TimeValue(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)
            );
        }

        return new GlobalFieldData(indexReader, fielddata, ramBytesUsed, perType);
    }

    private static class GlobalAtomicFieldData extends AbstractAtomicParentChildFieldData {

        private final Set<String> types;
        private final Map<String, OrdinalMapAndAtomicFieldData> atomicFD;
        private final int segmentIndex;

        public GlobalAtomicFieldData(Set<String> types, Map<String, OrdinalMapAndAtomicFieldData> atomicFD, int segmentIndex) {
            this.types = types;
            this.atomicFD = atomicFD;
            this.segmentIndex = segmentIndex;
        }

        @Override
        public Set<String> types() {
            return types;
        }

        @Override
        public SortedDocValues getOrdinalsValues(String type) {
            final OrdinalMapAndAtomicFieldData atomicFD = this.atomicFD.get(type);
            if (atomicFD == null) {
                return DocValues.emptySorted();
            }

            final OrdinalMap ordMap = atomicFD.ordMap;
            final SortedDocValues[] allSegmentValues = new SortedDocValues[atomicFD.fieldData.length];
            for (int i = 0; i < allSegmentValues.length; ++i) {
                allSegmentValues[i] = atomicFD.fieldData[i].getOrdinalsValues(type);
            }
            final SortedDocValues segmentValues = allSegmentValues[segmentIndex];
            if (segmentValues.getValueCount() == ordMap.getValueCount()) {
                // ords are already global
                return segmentValues;
            }
            final LongValues globalOrds = ordMap.getGlobalOrds(segmentIndex);
            return new SortedDocValues() {

                @Override
                public BytesRef lookupOrd(int ord) {
                    final int segmentIndex = ordMap.getFirstSegmentNumber(ord);
                    final int segmentOrd = (int) ordMap.getFirstSegmentOrd(ord);
                    return allSegmentValues[segmentIndex].lookupOrd(segmentOrd);
                }

                @Override
                public int getValueCount() {
                    return (int) ordMap.getValueCount();
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
        }

        @Override
        public long ramBytesUsed() {
            // this class does not take memory on its own, the index-level field data does
            // it through the use of ordinal maps
            return 0;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public void close() {
            List<Releasable> closeables = new ArrayList<>();
            for (OrdinalMapAndAtomicFieldData fds : atomicFD.values()) {
                closeables.addAll(Arrays.asList(fds.fieldData));
            }
            Releasables.close(closeables);
        }

    }

    public class GlobalFieldData implements IndexParentChildFieldData, Accountable {

        private final Object coreCacheKey;
        private final List<LeafReaderContext> leaves;
        private final AtomicParentChildFieldData[] fielddata;
        private final long ramBytesUsed;
        private final Map<String, OrdinalMapAndAtomicFieldData> ordinalMapPerType;

        GlobalFieldData(IndexReader reader, AtomicParentChildFieldData[] fielddata, long ramBytesUsed, Map<String, OrdinalMapAndAtomicFieldData> ordinalMapPerType) {
            this.coreCacheKey = reader.getCoreCacheKey();
            this.leaves = reader.leaves();
            this.ramBytesUsed = ramBytesUsed;
            this.fielddata = fielddata;
            this.ordinalMapPerType = ordinalMapPerType;
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
        public AtomicParentChildFieldData load(LeafReaderContext context) {
            assert context.reader().getCoreCacheKey() == leaves.get(context.ord).reader().getCoreCacheKey();
            return fielddata[context.ord];
        }

        @Override
        public AtomicParentChildFieldData loadDirect(LeafReaderContext context) throws Exception {
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
        public Index index() {
            return ParentChildIndexFieldData.this.index();
        }

        @Override
        public long ramBytesUsed() {
            return ramBytesUsed;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public IndexParentChildFieldData loadGlobal(DirectoryReader indexReader) {
            if (indexReader.getCoreCacheKey() == coreCacheKey) {
                return this;
            }
            throw new IllegalStateException();
        }

        @Override
        public IndexParentChildFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
            return loadGlobal(indexReader);
        }

    }

    /**
     * Returns the global ordinal map for the specified type
     */
    // TODO: OrdinalMap isn't expose in the field data framework, because it is an implementation detail.
    // However the JoinUtil works directly with OrdinalMap, so this is a hack to get access to OrdinalMap
    // I don't think we should expose OrdinalMap in IndexFieldData, because only parent/child relies on it and for the
    // rest of the code OrdinalMap is an implementation detail, but maybe we can expose it in IndexParentChildFieldData interface?
    public static MultiDocValues.OrdinalMap getOrdinalMap(IndexParentChildFieldData indexParentChildFieldData, String type) {
        if (indexParentChildFieldData instanceof ParentChildIndexFieldData.GlobalFieldData) {
            return ((GlobalFieldData) indexParentChildFieldData).ordinalMapPerType.get(type).ordMap;
        } else {
            // one segment, local ordinals are global
            return null;
        }
    }

}
