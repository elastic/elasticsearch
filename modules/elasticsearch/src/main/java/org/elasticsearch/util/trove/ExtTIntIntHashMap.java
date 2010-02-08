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

package org.elasticsearch.util.trove;

import org.elasticsearch.util.gnu.trove.TIntHashingStrategy;
import org.elasticsearch.util.gnu.trove.TIntIntHashMap;

/**
 * @author kimchy (Shay Banon)
 */
public class ExtTIntIntHashMap extends TIntIntHashMap {

    private int defaultReturnValue = 0;

    public ExtTIntIntHashMap() {
    }

    public ExtTIntIntHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    public ExtTIntIntHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public ExtTIntIntHashMap(TIntHashingStrategy strategy) {
        super(strategy);
    }

    public ExtTIntIntHashMap(int initialCapacity, TIntHashingStrategy strategy) {
        super(initialCapacity, strategy);
    }

    public ExtTIntIntHashMap(int initialCapacity, float loadFactor, TIntHashingStrategy strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    public ExtTIntIntHashMap defaultReturnValue(int defaultReturnValue) {
        this.defaultReturnValue = defaultReturnValue;
        return this;
    }

    @Override public int get(int key) {
        int index = index(key);
        return index < 0 ? defaultReturnValue : _values[index];
    }


}
