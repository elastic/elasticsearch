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

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.BytesRef;

/**
 * A wrapped to {@link BytesRef} that also caches the hashCode for it.
 */
public class HashedBytesRef {

    public BytesRef bytes;
    public int hash;

    public HashedBytesRef() {
    }

    public HashedBytesRef(String bytes) {
        this(new BytesRef(bytes));
    }

    public HashedBytesRef(BytesRef bytes) {
        this(bytes, bytes.hashCode());
    }

    public HashedBytesRef(BytesRef bytes, int hash) {
        this.bytes = bytes;
        this.hash = hash;
    }

    public HashedBytesRef resetHashCode() {
        this.hash = bytes.hashCode();
        return this;
    }

    public HashedBytesRef reset(BytesRef bytes, int hash) {
        this.bytes = bytes;
        this.hash = hash;
        return this;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof HashedBytesRef) {
            return bytes.equals(((HashedBytesRef) other).bytes);
        }
        return false;
    }

    @Override
    public String toString() {
        return bytes.toString();
    }

    public HashedBytesRef deepCopy() {
        return deepCopyOf(this);
    }

    public static HashedBytesRef deepCopyOf(HashedBytesRef other) {
        BytesRef copy = BytesRef.deepCopyOf(other.bytes);
        return new HashedBytesRef(copy, other.hash);
    }
}
