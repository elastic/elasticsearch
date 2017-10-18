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

package org.elasticsearch.transport.nio;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.BytesPage;

import java.nio.ByteBuffer;

public class BytesPageNetworkBytes extends NetworkBytesReference2 {

    private final BytesArray bytesArray;

    public BytesPageNetworkBytes(BytesPage bytesPage) {
        super(bytesPage, bytesPage.getByteArray().length);
        this.bytesArray = new BytesArray(bytesPage.getByteArray());
    }

    private BytesPageNetworkBytes(BytesArray bytesArray, RefCountedReleasable releasable) {
        super(releasable, bytesArray.length());
        this.bytesArray = bytesArray;
    }

    @Override
    public NetworkBytesReference2 sliceAndRetain(int from, int length) {
        BytesArray slice = (BytesArray) bytesArray.slice(from, length);
        refCountedReleasable.incRef();
        return new BytesPageNetworkBytes(slice, refCountedReleasable);
    }

    @Override
    public ByteBuffer getWriteByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytesArray.array());
        byteBuffer.position(bytesArray.offset() + writeIndex);
        byteBuffer.limit(bytesArray.offset() + length);
        return byteBuffer;
    }

    @Override
    public ByteBuffer getReadByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytesArray.array());
        byteBuffer.position(bytesArray.offset() + readIndex);
        byteBuffer.limit(bytesArray.offset() + writeIndex);
        return byteBuffer;
    }

    @Override
    public byte get(int index) {
        return bytesArray.get(index);
    }

    @Override
    public int length() {
        return bytesArray.length();
    }

    @Override
    public BytesReference slice(int from, int length) {
        return bytesArray.slice(from, length);
    }

    @Override
    public BytesRef toBytesRef() {
        return bytesArray.toBytesRef();
    }

    @Override
    public long ramBytesUsed() {
        return bytesArray.ramBytesUsed();
    }
}
