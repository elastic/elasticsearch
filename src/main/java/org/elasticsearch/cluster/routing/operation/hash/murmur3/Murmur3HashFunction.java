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

package org.elasticsearch.cluster.routing.operation.hash.murmur3;

import org.elasticsearch.cluster.routing.operation.hash.HashFunction;
import org.elasticsearch.common.hash.MurmurHash3;

/**
 * Hash function based on the Murmur3 algorithm, which is the default as of Elasticsearch 2.0.
 */
public class Murmur3HashFunction implements HashFunction {

    @Override
    public int hash(String routing) {
        final byte[] bytesToHash = new byte[routing.length() * 2];
        for (int i = 0; i < routing.length(); ++i) {
            final char c = routing.charAt(i);
            final byte b1 = (byte) (c >>> 8), b2 = (byte) c;
            assert ((b1 & 0xFF) << 8 | (b2 & 0xFF)) == c; // no information loss
            bytesToHash[i * 2] = b1;
            bytesToHash[i * 2 + 1] = b2;
        }
        final MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytesToHash, 0, bytesToHash.length, 0, new MurmurHash3.Hash128());
        return (int) hash.h1;
    }

    @Override
    public int hash(String type, String id) {
        throw new UnsupportedOperationException();
    }

}
