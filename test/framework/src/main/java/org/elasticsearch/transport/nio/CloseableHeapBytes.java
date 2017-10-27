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

public class CloseableHeapBytes extends BytesReference implements Releasable {

    private final BytesArray bytesArray;
    private final Releasable releasable;

    private CloseableHeapBytes(BytesArray bytesArray, Releasable releasable) {
        this.bytesArray = bytesArray;
        this.releasable = releasable;
    }

    private CloseableHeapBytes(BytesArray bytesArray) {
        this.releasable = null;
        this.bytesArray = bytesArray;
    }

    public static CloseableHeapBytes fromBytesPage(BytesPage bytesPage) {
        return new CloseableHeapBytes(new BytesArray(bytesPage.getByteArray()), bytesPage);
    }

    public static CloseableHeapBytes wrap(BytesArray bytesArray) {
        return new CloseableHeapBytes(bytesArray);
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

    @Override
    public void close() {
        if (releasable != null) {
            releasable.close();
        }
    }
}
