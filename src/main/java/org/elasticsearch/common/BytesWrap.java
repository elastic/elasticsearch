/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import java.util.Arrays;

/**
 *
 */
public class BytesWrap {

    private final byte[] bytes;

    // we pre-compute the hashCode for better performance (especially in IdCache)
    private final int hashCode;

    public BytesWrap(byte[] bytes) {
        this.bytes = bytes;
        this.hashCode = Arrays.hashCode(bytes);
    }

    public BytesWrap(String str) {
        this(Unicode.fromStringAsBytes(str));
    }

    public byte[] bytes() {
        return this.bytes;
    }

    public String utf8ToString() {
        return Unicode.fromBytes(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        BytesWrap bytesWrap = (BytesWrap) o;
        return Arrays.equals(bytes, bytesWrap.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }
}
