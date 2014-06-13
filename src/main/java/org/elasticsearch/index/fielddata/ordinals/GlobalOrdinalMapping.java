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

import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;

/**
 * A {@link BytesValues.WithOrdinals} implementation that returns ordinals that are global.
 */
public class GlobalOrdinalMapping extends BytesValues.WithOrdinals {

    private final BytesValues.WithOrdinals values;
    private final OrdinalMap ordinalMap;
    private final BytesValues.WithOrdinals[] bytesValues;
    private final int segmentIndex;

    GlobalOrdinalMapping(OrdinalMap ordinalMap, BytesValues.WithOrdinals[] bytesValues, int segmentIndex) {
        super(bytesValues[segmentIndex].isMultiValued());
        this.values = bytesValues[segmentIndex];
        this.segmentIndex = segmentIndex;
        this.bytesValues = bytesValues;
        this.ordinalMap = ordinalMap;
    }

    int readerIndex;

    @Override
    public BytesRef copyShared() {
        return bytesValues[readerIndex].copyShared();
    }

    @Override
    public long getMaxOrd() {
        return ordinalMap.getValueCount();
    }

    public long getGlobalOrd(long segmentOrd) {
        return segmentOrd == MISSING_ORDINAL ? MISSING_ORDINAL : ordinalMap.getGlobalOrd(segmentIndex, segmentOrd);
    }

    @Override
    public long getOrd(int docId) {
        return getGlobalOrd(values.getOrd(docId));
    }

    @Override
    public long nextOrd() {
        return getGlobalOrd(values.nextOrd());
    }

    @Override
    public long currentOrd() {
        return getGlobalOrd(values.currentOrd());
    }

    @Override
    public int setDocument(int docId) {
        return values.setDocument(docId);
    }

    @Override
    public BytesRef getValueByOrd(long globalOrd) {
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
        readerIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
        return bytesValues[readerIndex].getValueByOrd(segmentOrd);
    }

}
