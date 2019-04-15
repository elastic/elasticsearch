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

public interface GlobalOrdinalMap {

    /**
     * Given a segment number, return a {@link LongValues} instance that maps
     * segment ordinals to global ordinals.
     */
    LongValues getGlobalOrds(int segmentIndex);

    /**
     * Returns the total number of unique terms in global ord space.
     */
    long getValueCount();

    OrdinalMap getWrappedMap();

    static GlobalOrdinalMap create(OrdinalMap ordinalMap) {
        return new OrdinalMapWrapper(ordinalMap);
    }

    class OrdinalMapWrapper implements GlobalOrdinalMap {
        private final OrdinalMap ordinalMap;

        private OrdinalMapWrapper(OrdinalMap ordinalMap) {
            this.ordinalMap = ordinalMap;
        }

        /**
         * Given a segment number, return a {@link LongValues} instance that maps
         * segment ordinals to global ordinals.
         */
        public LongValues getGlobalOrds(int segmentIndex) {
            return ordinalMap.getGlobalOrds(segmentIndex);
        }

        /**
         * Returns the total number of unique terms in global ord space.
         */
        public long getValueCount() {
            return ordinalMap.getValueCount();
        }

        public OrdinalMap getWrappedMap() {
            return ordinalMap;
        }
    }
}
