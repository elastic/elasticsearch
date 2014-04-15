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
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.ordinals.InternalGlobalOrdinalsBuilder.OrdinalMappingSource;
import org.elasticsearch.index.fielddata.plain.AtomicFieldDataWithOrdinalsTermsEnum;
import org.elasticsearch.index.mapper.FieldMapper;

/**
 * {@link org.elasticsearch.index.fielddata.IndexFieldData} impl based on global ordinals.
 */
final class InternalGlobalOrdinalsIndexFieldData extends GlobalOrdinalsIndexFieldData {

    private final Atomic[] atomicReaders;

    InternalGlobalOrdinalsIndexFieldData(Index index, Settings settings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, AtomicFieldData.WithOrdinals[] segmentAfd, LongValues globalOrdToFirstSegment, LongValues globalOrdToFirstSegmentDelta, OrdinalMappingSource[] segmentOrdToGlobalOrds, long memorySizeInBytes) {
        super(index, settings, fieldNames, fieldDataType, memorySizeInBytes);
        this.atomicReaders = new Atomic[segmentAfd.length];
        for (int i = 0; i < segmentAfd.length; i++) {
            atomicReaders[i] = new Atomic(segmentAfd[i], globalOrdToFirstSegment, globalOrdToFirstSegmentDelta, segmentOrdToGlobalOrds[i]);
        }
    }

    @Override
    public AtomicFieldData.WithOrdinals load(AtomicReaderContext context) {
        return atomicReaders[context.ord];
    }

    private final class Atomic implements AtomicFieldData.WithOrdinals {

        private final WithOrdinals afd;
        private final OrdinalMappingSource segmentOrdToGlobalOrdLookup;
        private final LongValues globalOrdToFirstSegment;
        private final LongValues globalOrdToFirstSegmentDelta;

        private Atomic(WithOrdinals afd, LongValues globalOrdToFirstSegment, LongValues globalOrdToFirstSegmentDelta, OrdinalMappingSource segmentOrdToGlobalOrdLookup) {
            this.afd = afd;
            this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
            this.globalOrdToFirstSegment = globalOrdToFirstSegment;
            this.globalOrdToFirstSegmentDelta = globalOrdToFirstSegmentDelta;
        }

        @Override
        public BytesValues.WithOrdinals getBytesValues(boolean needsHashes) {
            BytesValues.WithOrdinals values = afd.getBytesValues(false);
            Ordinals.Docs segmentOrdinals = values.ordinals();
            final Ordinals.Docs globalOrdinals;
            if (segmentOrdToGlobalOrdLookup != null) {
                globalOrdinals = segmentOrdToGlobalOrdLookup.globalOrdinals(segmentOrdinals);
            } else {
                globalOrdinals = segmentOrdinals;
            }
            final BytesValues.WithOrdinals[] bytesValues = new BytesValues.WithOrdinals[atomicReaders.length];
            for (int i = 0; i < bytesValues.length; i++) {
                bytesValues[i] = atomicReaders[i].afd.getBytesValues(false);
            }
            return new BytesValues.WithOrdinals(globalOrdinals) {

                int readerIndex;

                @Override
                public BytesRef getValueByOrd(long globalOrd) {
                    final long segmentOrd = globalOrd - globalOrdToFirstSegmentDelta.get(globalOrd);
                    readerIndex = (int) globalOrdToFirstSegment.get(globalOrd);
                    return bytesValues[readerIndex].getValueByOrd(segmentOrd);
                }

                @Override
                public BytesRef copyShared() {
                    return bytesValues[readerIndex].copyShared();
                }

                @Override
                public int currentValueHash() {
                    return bytesValues[readerIndex].currentValueHash();
                }
            };
        }

        @Override
        public boolean isMultiValued() {
            return afd.isMultiValued();
        }

        @Override
        public long getNumberUniqueValues() {
            return afd.getNumberUniqueValues();
        }

        @Override
        public long getMemorySizeInBytes() {
            return afd.getMemorySizeInBytes();
        }

        @Override
        public ScriptDocValues getScriptValues() {
            throw new UnsupportedOperationException("Script values not supported on global ordinals");
        }

        @Override
        public TermsEnum getTermsEnum() {
            return new AtomicFieldDataWithOrdinalsTermsEnum(this);
        }

        @Override
        public void close() {
        }

    }

}
