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
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public abstract class NetworkBytesReference extends BytesReference {

    protected int length;
    protected int writeIndex;
    protected int readIndex;

    public abstract NetworkBytesReference slice(int from, int length);

    public abstract int getWriteIndex();

    public abstract void incrementWrite(int delta);

    public abstract int getWriteRemaining();

    public abstract int getReadIndex();

    public abstract void incrementRead(int delta);

    public abstract int getReadRemaining();

    public abstract boolean isComposite();

    public abstract ByteBuffer getWriteByteBuffer();

    public abstract ByteBuffer getReadByteBuffer();

    public abstract ByteBuffer[] getWriteByteBuffers();

    public abstract ByteBuffer[] getReadByteBuffers();

    public static NetworkBytesReference fromBytesReference(BytesReference reference) {
        BytesRefIterator byteRefIterator = reference.iterator();
        BytesRef r;
        try {
            // Most network messages are composed of three buffers
            ArrayList<ByteBufferReference> references = new ArrayList<>(3);
            while ((r = byteRefIterator.next()) != null) {
                references.add(ByteBufferReference.heapBuffer(new BytesArray(r), r.length, 0));
            }
            if (references.size() == 1) {
                return references.get(0);
            } else {
                ByteBufferReference[] newReferences = references.toArray(new ByteBufferReference[references.size()]);
                return new CompositeByteBufferReference(newReferences);
            }
        } catch (IOException e) {
            // this is really an error since we don't do IO in our bytesreferences
            throw new AssertionError("won't happen", e);
        }
    }
}
