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

import org.apache.lucene.index.*;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
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
public class ParentChildIndexFieldData extends AbstractIndexFieldData<AtomicParentChildFieldData> implements IndexParentChildFieldData {

    private final Set<String> parentTypes;
    private final CircuitBreakerService breakerService;

    public ParentChildIndexFieldData(Index index, @IndexSettings Settings indexSettings, MappedFieldType.Names fieldNames,
                                     FieldDataType fieldDataType, IndexFieldDataCache cache, MapperService mapperService,
                                     CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        this.breakerService = breakerService;
        Set<String> parentTypes = new HashSet<>();
        for (DocumentMapper mapper : mapperService.docMappers(false)) {
            ParentFieldMapper parentFieldMapper = mapper.parentFieldMapper();
            if (parentFieldMapper.active()) {
                parentTypes.add(parentFieldMapper.type());
            }
        }
        this.parentTypes = parentTypes;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    public AtomicParentChildFieldData load(LeafReaderContext context) {
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
    }

    @Override
    public AbstractAtomicParentChildFieldData loadDirect(LeafReaderContext context) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AtomicParentChildFieldData empty(int maxDoc) {
        return AbstractAtomicParentChildFieldData.empty();
    }

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<?> build(Index index, @IndexSettings Settings indexSettings, MappedFieldType fieldType,
                                       IndexFieldDataCache cache, CircuitBreakerService breakerService,
                                       MapperService mapperService) {
            return new ParentChildIndexFieldData(index, indexSettings, fieldType.names(), fieldType.fieldDataType(), cache,
                    mapperService, breakerService);
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
    public IndexParentChildFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
        final long startTime = System.nanoTime();

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

        private final AtomicParentChildFieldData[] fielddata;
        private final IndexReader reader;
        private final long ramBytesUsed;
        private final Map<String, OrdinalMapAndAtomicFieldData> ordinalMapPerType;

        GlobalFieldData(IndexReader reader, AtomicParentChildFieldData[] fielddata, long ramBytesUsed, Map<String, OrdinalMapAndAtomicFieldData> ordinalMapPerType) {
            this.reader = reader;
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
            assert context.reader().getCoreCacheKey() == reader.leaves().get(context.ord).reader().getCoreCacheKey();
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
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public IndexParentChildFieldData loadGlobal(IndexReader indexReader) {
            if (indexReader.getCoreCacheKey() == reader.getCoreCacheKey()) {
                return this;
            }
            throw new IllegalStateException();
        }

        @Override
        public IndexParentChildFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
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
