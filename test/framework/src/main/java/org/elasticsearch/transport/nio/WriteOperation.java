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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.util.ArrayList;

public class WriteOperation {

    private final NioSocketChannel channel;
    private final ActionListener<Void> listener;
    private final NetworkBytesReference[] references;

    public WriteOperation(NioSocketChannel channel, BytesReference bytesReference, ActionListener<Void> listener) {
        this.channel = channel;
        this.listener = listener;
        this.references = toArray(bytesReference);
    }

    public NetworkBytesReference[] getByteReferences() {
        return references;
    }

    public ActionListener<Void> getListener() {
        return listener;
    }

    public NioSocketChannel getChannel() {
        return channel;
    }

    public boolean isFullyFlushed() {
        return references[references.length - 1].hasReadRemaining() == false;
    }

    public int flush() throws IOException {
        return channel.write(references);
    }

    private static NetworkBytesReference[] toArray(BytesReference reference) {
        BytesRefIterator byteRefIterator = reference.iterator();
        BytesRef r;
        try {
            // Most network messages are composed of three buffers
            ArrayList<NetworkBytesReference> references = new ArrayList<>(3);
            while ((r = byteRefIterator.next()) != null) {
                references.add(NetworkBytesReference.wrap(new BytesArray(r), r.length, 0));
            }
            return references.toArray(new NetworkBytesReference[references.size()]);

        } catch (IOException e) {
            // this is really an error since we don't do IO in our bytesreferences
            throw new AssertionError("won't happen", e);
        }
    }
}
