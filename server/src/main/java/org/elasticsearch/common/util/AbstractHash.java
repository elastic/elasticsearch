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

package org.elasticsearch.common.util;

import org.elasticsearch.common.lease.Releasables;

/**
 * Base implementation for {@link BytesRefHash} and {@link LongHash}, or any class that
 * needs to map values to dense ords. This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
abstract class AbstractHash extends AbstractPagedHashMap {

    LongArray ids;

    AbstractHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        ids = bigArrays.newLongArray(capacity(), true);
    }

    /**
     * Get the id associated with key at <code>0 &lt;= index &lt;= capacity()</code> or -1 if this slot is unused.
     */
    public long id(long index) {
        return ids.get(index) - 1;
    }

    protected final long id(long index, long id) {
        return ids.set(index, id + 1) - 1;
    }

    @Override
    protected void resize(long capacity) {
        ids = bigArrays.resize(ids, capacity);
    }

    @Override
    protected boolean used(long bucket) {
        return id(bucket) >= 0;
    }

    @Override
    public void close() {
        Releasables.close(ids);
    }
}
