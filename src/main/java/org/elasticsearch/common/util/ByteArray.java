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

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;

/**
 * Abstraction of an array of byte values.
 */
public interface ByteArray extends BigArray {

    /**
     * Get an element given its index.
     */
    public abstract byte get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    public abstract byte set(long index, byte value);

    /**
     * Get a reference to a slice.
     */
    public abstract void get(long index, int len, BytesRef ref);

    /**
     * Bulk set.
     */
    public abstract void set(long index, byte[] buf, int offset, int len);

}
