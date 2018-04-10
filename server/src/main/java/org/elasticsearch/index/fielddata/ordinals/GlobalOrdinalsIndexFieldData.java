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
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;
import org.elasticsearch.search.MultiValueMode;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

/**
 * {@link IndexFieldData} base class for concrete global ordinals implementations.
 */
public class GlobalOrdinalsIndexFieldData extends AbstractIndexComponent implements IndexOrdinalsFieldData, Accountable {

    private final String fieldName;
    private final long memorySizeInBytes;

    private final OrdinalMap ordinalMap;
    private final Atomic[] atomicReaders;
    private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;


    protected GlobalOrdinalsIndexFieldData(IndexSettings indexSettings, String fieldName, AtomicOrdinalsFieldData[] segmentAfd,
                                           OrdinalMap ordinalMap, long memorySizeInBytes, Function<SortedSetDocValues,
                                           ScriptDocValues<?>> scriptFunction) {
        super(indexSettings);
        this.fieldName = fieldName;
        this.memorySizeInBytes = memorySizeInBytes;
        this.ordinalMap = ordinalMap;
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(segmentAfd[i], ordinalMap, i);
        }
        this.scriptFunction = scriptFunction;
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return this;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        throw new UnsupportedOperationException("no global ordinals sorting yet");
    }

    @Override
    public void clear() {
        // no need to clear, because this is cached and cleared in AbstractBytesIndexFieldData
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
    public AtomicOrdinalsFieldData load(LeafReaderContext context) {
        return atomicReaders[context.ord];
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return ordinalMap;
    }

    private final class Atomic extends AbstractAtomicOrdinalsFieldData {

        private final AtomicOrdinalsFieldData afd;
        private final OrdinalMap ordinalMap;
        private final int segmentIndex;

        private Atomic(AtomicOrdinalsFieldData afd, OrdinalMap ordinalMap, int segmentIndex) {
            super(scriptFunction);
            this.afd = afd;
            this.ordinalMap = ordinalMap;
            this.segmentIndex = segmentIndex;
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            final SortedSetDocValues values = afd.getOrdinalsValues();
            if (values.getValueCount() == ordinalMap.getValueCount()) {
                // segment ordinals match global ordinals
                return values;
            }
            final SortedSetDocValues[] bytesValues = new SortedSetDocValues[atomicReaders.length];
            for (int i = 0; i < bytesValues.length; i++) {
                bytesValues[i] = atomicReaders[i].afd.getOrdinalsValues();
            }
            return new GlobalOrdinalMapping(ordinalMap, bytesValues, segmentIndex);
        }

        @Override
        public long ramBytesUsed() {
            return afd.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return afd.getChildResources();
        }

        @Override
        public void close() {
        }

    }
}
