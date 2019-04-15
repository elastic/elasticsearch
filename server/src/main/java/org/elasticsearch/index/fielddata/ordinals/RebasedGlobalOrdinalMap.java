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

import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.util.LongValues;

public class RebasedGlobalOrdinalMap implements GlobalOrdinalMap {

    private final GlobalOrdinalMap delegate;
    private final long minOrd;
    private final long maxOrd;

    public RebasedGlobalOrdinalMap(GlobalOrdinalMap delegate,
                                   long minOrd,
                                   long maxOrd) {
        this.delegate = delegate;
        this.minOrd = minOrd;
        this.maxOrd = maxOrd;
    }

    @Override
    public LongValues getGlobalOrds(int segmentIndex) {
        LongValues globalOrds = delegate.getGlobalOrds(segmentIndex);
        return new RebasedLongValues(globalOrds, minOrd, maxOrd);
    }

    @Override
    public long getValueCount() {
        return maxOrd - minOrd;
    }

    @Override
    public OrdinalMap getWrappedMap() {
        throw new IllegalArgumentException();
    }

    private static class RebasedLongValues extends LongValues {

        private final LongValues delegate;
        private final long minOrd;
        private final long maxOrd;

        RebasedLongValues(LongValues delegate,
                          long minOrd,
                          long maxOrd) {
            this.delegate = delegate;
            this.minOrd = minOrd;
            this.maxOrd = maxOrd;
        }

        @Override
        public long get(long index) {
            assert index <= maxOrd;
            return delegate.get(index) - minOrd;
        }
    }
}
