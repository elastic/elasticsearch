/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

/**
 * @author kimchy (shay.banon)
 */
public class ExtTObjectFloatHashMap<T> extends TObjectFloatHashMap<T> {

    private float defaultReturnValue = 0;

    public ExtTObjectFloatHashMap() {
    }

    public ExtTObjectFloatHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public ExtTObjectFloatHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public ExtTObjectFloatHashMap(TObjectHashingStrategy<T> ttObjectHashingStrategy) {
        super(ttObjectHashingStrategy);
    }

    public ExtTObjectFloatHashMap(int initialCapacity, TObjectHashingStrategy<T> ttObjectHashingStrategy) {
        super(initialCapacity, ttObjectHashingStrategy);
    }

    public ExtTObjectFloatHashMap(int initialCapacity, float loadFactor, TObjectHashingStrategy<T> ttObjectHashingStrategy) {
        super(initialCapacity, loadFactor, ttObjectHashingStrategy);
    }

    public ExtTObjectFloatHashMap<T> defaultReturnValue(float defaultReturnValue) {
        this.defaultReturnValue = defaultReturnValue;
        return this;
    }

    @Override public float get(T key) {
        int index = index(key);
        return index < 0 ? defaultReturnValue : _values[index];
    }
}
