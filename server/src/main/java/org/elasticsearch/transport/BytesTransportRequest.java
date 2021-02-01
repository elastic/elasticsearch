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

package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.RefCounted;

import java.io.IOException;

/**
 * A specialized, bytes only request, that can potentially be optimized on the network
 * layer, specifically for the same large buffer send to several nodes.
 */
public class BytesTransportRequest extends TransportRequest implements RefCounted {

    final ReleasableBytesReference bytes;
    private final Version version;

    public BytesTransportRequest(StreamInput in) throws IOException {
        super(in);
        bytes = in.readReleasableBytesReference();
        version = in.getVersion();
    }

    public BytesTransportRequest(BytesReference bytes, Version version) {
        this.bytes = ReleasableBytesReference.wrap(bytes);
        this.version = version;
    }

    public Version version() {
        return this.version;
    }

    public BytesReference bytes() {
        return this.bytes;
    }

    /**
     * Writes the data in a "thin" manner, without the actual bytes, assumes
     * the actual bytes will be appended right after this content.
     */
    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(bytes.length());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(bytes);
    }

    @Override
    public void incRef() {
        bytes.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return bytes.decRef();
    }

    @Override
    public boolean decRef() {
        return bytes.decRef();
    }
}
