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

import org.apache.lucene.index.XOrdinalMap;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.elasticsearch.index.fielddata.BytesValues;

/**
 * A {@link BytesValues.WithOrdinals} implementation that returns ordinals that are global.
 */
public class GlobalOrdinalMapping extends BytesValues.WithOrdinals {

    private final BytesValues.WithOrdinals values;
    private final XOrdinalMap ordinalMap;
    private final LongValues mapping;
    private final BytesValues.WithOrdinals[] bytesValues;

    GlobalOrdinalMapping(XOrdinalMap ordinalMap, BytesValues.WithOrdinals[] bytesValues, int segmentIndex) {
        super(bytesValues[segmentIndex].isMultiValued());
        this.values = bytesValues[segmentIndex];
        this.bytesValues = bytesValues;
        this.ordinalMap = ordinalMap;
        this.mapping = ordinalMap.getGlobalOrds(segmentIndex);
    }

    @Override
    public long getMaxOrd() {
        return ordinalMap.getValueCount();
    }
    
    // NOTE: careful if we change the API here: unnecessary branch for < 0 here hurts a lot. 
    // so if we already know the count (from setDocument), its bad to do it redundantly.

    public long getGlobalOrd(long segmentOrd) {
        return mapping.get(segmentOrd);
    }

    @Override
    public long getOrd(int docId) {
        long v = values.getOrd(docId);
        if (v < 0) {
            return v;
        } else {
            return getGlobalOrd(v);
        }
    }

    @Override
    public long nextOrd() {
        return getGlobalOrd(values.nextOrd());
    }

    @Override
    public int setDocument(int docId) {
        return values.setDocument(docId);
    }

    @Override
    public BytesRef getValueByOrd(long globalOrd) {
        final long segmentOrd = ordinalMap.getFirstSegmentOrd(globalOrd);
        int readerIndex = ordinalMap.getFirstSegmentNumber(globalOrd);
        return bytesValues[readerIndex].getValueByOrd(segmentOrd);
    }

}
