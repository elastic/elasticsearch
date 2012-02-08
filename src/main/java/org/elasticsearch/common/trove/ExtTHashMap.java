/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.trove;

import gnu.trove.map.hash.THashMap;

import java.util.Map;

public class ExtTHashMap<K, V> extends THashMap<K, V> {

    public ExtTHashMap() {
    }

    public ExtTHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public ExtTHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public ExtTHashMap(Map<K, V> kvMap) {
        super(kvMap);
    }

    public ExtTHashMap(THashMap<K, V> kvtHashMap) {
        super(kvtHashMap);
    }

    /**
     * Internal method to get the actual values associated. Some values might have "null" or no entry
     * values.
     */
    public Object[] internalValues() {
        return this._values;
    }
}