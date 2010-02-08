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

package org.elasticsearch.util.lucene.versioned;

import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.concurrent.highscalelib.NonBlockingHashMapLong;

/**
 * An implementation of {@link VersionedMap} based on {@link NonBlockingHashMapLong}.
 *
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class NonBlockingVersionedMap implements VersionedMap {

    private final NonBlockingHashMapLong<Integer> map = new NonBlockingHashMapLong<Integer>();

    @Override public boolean beforeVersion(int key, int versionToCheck) {
        Integer result = map.get(key);
        return result == null || versionToCheck < result;
    }

    @Override public void putVersion(int key, int version) {
        map.put(key, version);
    }

    @Override public void putVersionIfAbsent(int key, int version) {
        map.putIfAbsent(key, version);
    }

    @Override public void clear() {
        map.clear();
    }
}
