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
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BytesPage;

import java.nio.ByteBuffer;

public class HeapNetworkBytes extends NetworkBytesReference {

    private final BytesArray bytesArray;

    private HeapNetworkBytes(BytesArray bytesArray, Releasable releasable) {
        super(releasable, bytesArray.length());
        this.bytesArray = bytesArray;
    }

    private HeapNetworkBytes(BytesArray bytesArray, int index) {
        super(null, bytesArray.length());
        this.bytesArray = bytesArray;
        this.index = index;
    }

    private HeapNetworkBytes(BytesArray bytesArray, Releasable releasable, int index) {
        super(releasable, bytesArray.length());
        this.bytesArray = bytesArray;
        this.index = index;
    }

    public static HeapNetworkBytes fromBytesPage(BytesPage bytesPage) {
        return new HeapNetworkBytes(new BytesArray(bytesPage.getByteArray()), bytesPage);
    }

    public static HeapNetworkBytes wrap(BytesArray bytesArray) {
        return new HeapNetworkBytes(bytesArray, 0);
    }

    public static NetworkBytesReference wrap(BytesArray bytesArray, int index) {
        return new HeapNetworkBytes(bytesArray, index);
    }

    @Override
    public NetworkBytesReference sliceAndRetain(int from, int length) {
        BytesArray slice = bytesArray.slice(from, length);
        int newIndex = Math.min(Math.max(index - from, 0), length);
        return new HeapNetworkBytes(slice, releasable, newIndex);
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    @Override
    public ByteBuffer postIndexByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytesArray.array());
        byteBuffer.position(bytesArray.offset() + index);
        byteBuffer.limit(bytesArray.offset() + length);
        return byteBuffer;
    }

    @Override
    public ByteBuffer preIndexByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytesArray.array());
        byteBuffer.position(bytesArray.offset());
        byteBuffer.limit(bytesArray.offset() + index);
        return byteBuffer;
    }

    @Override
    public ByteBuffer[] postIndexByteBuffers() {
        ByteBuffer[] byteBuffers = new ByteBuffer[1];
        byteBuffers[0] = postIndexByteBuffer();
        return byteBuffers;
    }

    @Override
    public ByteBuffer[] preIndexByteBuffers() {
        ByteBuffer[] byteBuffers = new ByteBuffer[1];
        byteBuffers[0] = preIndexByteBuffer();
        return byteBuffers;
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
