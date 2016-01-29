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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collection;

/**
 * {@link org.elasticsearch.index.fielddata.IndexFieldData} impl based on global ordinals.
 */
final class InternalGlobalOrdinalsIndexFieldData extends GlobalOrdinalsIndexFieldData {

    private final Atomic[] atomicReaders;

    InternalGlobalOrdinalsIndexFieldData(IndexSettings indexSettings, String fieldName, FieldDataType fieldDataType, AtomicOrdinalsFieldData[] segmentAfd, OrdinalMap ordinalMap, long memorySizeInBytes) {
        super(indexSettings, fieldName, fieldDataType, memorySizeInBytes);
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(segmentAfd[i], ordinalMap, i);
        }
    }

    @Override
    public AtomicOrdinalsFieldData load(LeafReaderContext context) {
        return atomicReaders[context.ord];
    }

    private final class Atomic extends AbstractAtomicOrdinalsFieldData {

        private final AtomicOrdinalsFieldData afd;
        private final OrdinalMap ordinalMap;
        private final int segmentIndex;

        private Atomic(AtomicOrdinalsFieldData afd, OrdinalMap ordinalMap, int segmentIndex) {
            this.afd = afd;
            this.ordinalMap = ordinalMap;
            this.segmentIndex = segmentIndex;
        }

        @Override
        public RandomAccessOrds getOrdinalsValues() {
            final RandomAccessOrds values = afd.getOrdinalsValues();
            if (values.getValueCount() == ordinalMap.getValueCount()) {
                // segment ordinals match global ordinals
                return values;
            }
            final RandomAccessOrds[] bytesValues = new RandomAccessOrds[atomicReaders.length];
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
