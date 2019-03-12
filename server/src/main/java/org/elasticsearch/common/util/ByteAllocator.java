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

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.recycler.AbstractRecyclerC;
import org.elasticsearch.common.recycler.Recycler;

import static org.elasticsearch.common.recycler.Recyclers.none;

public class ByteAllocator {

    private final int pageSize;
    private final Recycler<byte[]> recycler;

    private ByteAllocator(int pageSize, Recycler<byte[]> recycler) {
        this.pageSize = pageSize;
        this.recycler = recycler;
    }

    public Bytes allocateBytePage() {
        return allocateBytes(pageSize);
    }

    public Bytes allocateBytes(int size) {
        if (size > pageSize) {
            return new Bytes(new byte[size], () -> {});
        } else {
            Recycler.V<byte[]> v = recycler.obtain();
            return new Bytes(v.v(), v);
        }
    }

    public int pageSize() {
        return pageSize;
    }

    public class Bytes implements Releasable {

        private final byte[] bytes;
        private final Releasable releasable;

        public Bytes(byte[] bytes, Releasable releasable) {
            this.bytes = bytes;
            this.releasable = releasable;
        }

        public byte[] bytes() {
            return bytes;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }

    private static AbstractRecyclerC<byte[]> recycler(int pageSize) {
        return new AbstractRecyclerC<byte[]>() {
            @Override
            public byte[] newInstance(int sizing) {
                return new byte[pageSize];
            }

            @Override
            public void recycle(byte[] value) {
            }
        };
    }

    public static ByteAllocator recyclingInstance(int pageSize, Recycler<byte[]> recycler) {
        return new ByteAllocator(pageSize, recycler);
    }

    public static ByteAllocator allocatingInstance(int pageSize) {
        return new ByteAllocator(pageSize, none(recycler(pageSize)));
    }
}
