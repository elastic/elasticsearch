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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.XOrdinalMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.FieldMapper;

/**
 * {@link org.elasticsearch.index.fielddata.IndexFieldData} impl based on global ordinals.
 */
final class InternalGlobalOrdinalsIndexFieldData extends GlobalOrdinalsIndexFieldData {

    private final Atomic[] atomicReaders;

    InternalGlobalOrdinalsIndexFieldData(Index index, Settings settings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, AtomicFieldData.WithOrdinals[] segmentAfd, XOrdinalMap ordinalMap, long memorySizeInBytes) {
        super(index, settings, fieldNames, fieldDataType, memorySizeInBytes);
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(segmentAfd[i], ordinalMap, i);
        }
    }

    @Override
    public AtomicFieldData.WithOrdinals load(AtomicReaderContext context) {
        return atomicReaders[context.ord];
    }

    private final class Atomic implements AtomicFieldData.WithOrdinals {

        private final WithOrdinals afd;
        private final XOrdinalMap ordinalMap;
        private final int segmentIndex;

        private Atomic(WithOrdinals afd, XOrdinalMap ordinalMap, int segmentIndex) {
            this.afd = afd;
            this.ordinalMap = ordinalMap;
            this.segmentIndex = segmentIndex;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues() {
            final BytesValues.WithOrdinals values = afd.getBytesValues();
            if (values.getMaxOrd() == ordinalMap.getValueCount()) {
                // segment ordinals match global ordinals
                return values;
            }
            final BytesValues.WithOrdinals[] bytesValues = new BytesValues.WithOrdinals[atomicReaders.length];
            for (int i = 0; i < bytesValues.length; i++) {
                bytesValues[i] = atomicReaders[i].afd.getBytesValues();
            }
            return new GlobalOrdinalMapping(ordinalMap, bytesValues, segmentIndex);
        }

        @Override
        public long ramBytesUsed() {
            return afd.ramBytesUsed();
        }

        @Override
        public ScriptDocValues getScriptValues() {
            throw new UnsupportedOperationException("Script values not supported on global ordinals");
        }

        @Override
        public void close() {
        }

    }

}
