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

import gnu.trove.map.hash.TObjectIntHashMap;

/**
 *
 */
public class ExtTObjectIntHasMap<T> extends TObjectIntHashMap<T> {

    public ExtTObjectIntHasMap() {
    }

    public ExtTObjectIntHasMap(int initialCapacity) {
        super(initialCapacity);
    }

    public ExtTObjectIntHasMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public ExtTObjectIntHasMap(int initialCapacity, float loadFactor, int noEntryValue) {
        super(initialCapacity, loadFactor, noEntryValue);
    }

    /**
     * Returns an already existing key, or <tt>null</tt> if it does not exists.
     */
    public T key(T key) {
        int index = index(key);
        return index < 0 ? null : (T) _set[index];
    }
}
