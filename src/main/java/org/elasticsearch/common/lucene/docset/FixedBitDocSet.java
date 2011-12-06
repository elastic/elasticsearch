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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.RamUsage;

import java.io.IOException;

/**
 *
 */
public class FixedBitDocSet extends DocSet {

    private final FixedBitSet set;

    public FixedBitDocSet(FixedBitSet set) {
        this.set = set;
    }

    public FixedBitDocSet(int numBits) {
        this.set = new FixedBitSet(numBits);
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public int length() {
        return set.length();
    }

    public FixedBitSet set() {
        return set;
    }

    @Override
    public boolean get(int doc) {
        return set.get(doc);
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
        return set.iterator();
    }

    @Override
    public long sizeInBytes() {
        return set.getBits().length * RamUsage.NUM_BYTES_LONG + RamUsage.NUM_BYTES_ARRAY_HEADER + RamUsage.NUM_BYTES_INT /* wlen */;
    }
}
