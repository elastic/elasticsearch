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
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.index.fielddata.AbstractRandomAccessOrds;

/**
 * A {@link RandomAccessOrds} implementation that returns ordinals that are global.
 */
public class GlobalOrdinalMapping extends AbstractRandomAccessOrds {

    private final RandomAccessOrds values;
    private final OrdinalMap ordinalMap;
    private final LongValues mapping;
    private final RandomAccessOrds[] bytesValues;

    GlobalOrdinalMapping(OrdinalMap ordinalMap, RandomAccessOrds[] bytesValues, int segmentIndex) {
        super();
        this.values = bytesValues[segmentIndex];
        this.bytesValues = bytesValues;
        this.ordinalMap = ordinalMap;
        this.mapping = ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public long getValueCount() {
        return ordinalMap.getValueCount();
    }

    public final long getGlobalOrd(long segmentOrd) {
        return mapping.get(segmentOrd);
    }

    @Override
    public long ordAt(int index) {
        return getGlobalOrd(values.ordAt(index));
    }

    @Override
    public void doSetDocument(int docId) {
        values.setDocument(docId);
    }

    @Override
    public int cardinality() {
        return values.cardinality();
    }

    @Override
    public BytesRef lookupOrd(long globalOrd) {
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
        int readerIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
        return bytesValues[readerIndex].lookupOrd(segmentOrd);
    }

}
