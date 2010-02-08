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

/**
 * A versioned map, allowing to put version numbers associated with specific
 * keys.
 * <p/>
 * <p>Note. versions can be assumed to be >= 0.
 *
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public interface VersionedMap {

    /**
     * Returns <tt>true</tt> if the versionToCheck is smaller than the current version
     * associated with the key. If there is no version associated with the key, then
     * it should return <tt>true</tt> as well.
     */
    boolean beforeVersion(int key, int versionToCheck);

    /**
     * Puts (and replaces if it exists) the current key with the provided version.
     */
    void putVersion(int key, int version);

    /**
     * Puts the version with the key only if it is absent.
     */
    void putVersionIfAbsent(int key, int version);

    /**
     * Clears the map.
     */
    void clear();
}
