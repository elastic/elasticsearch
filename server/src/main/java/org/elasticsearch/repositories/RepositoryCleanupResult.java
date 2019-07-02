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
package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.LongConsumer;

public final class RepositoryCleanupResult implements Writeable, ToXContentObject {

    private final long bytes;

    private final long blobs;

    private RepositoryCleanupResult(long bytes, long blobs) {
        this.bytes = bytes;
        this.blobs = blobs;
    }

    public RepositoryCleanupResult(StreamInput in) throws IOException {
        bytes = in.readLong();
        blobs = in.readLong();
    }

    public long bytes() {
        return bytes;
    }

    public long blobs() {
        return blobs;
    }

    public static Progress start() {
        return new Progress();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(bytes);
        out.writeLong(blobs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("bytes", bytes).field("blobs", blobs).endObject();
    }

    public static final class Progress implements LongConsumer {

        private long bytesCounter;

        private long blobsCounter;

        @Override
        public void accept(long size) {
            synchronized (this) {
                ++blobsCounter;
                bytesCounter += size;
            }
        }

        public RepositoryCleanupResult finish() {
            synchronized (this) {
                return new RepositoryCleanupResult(bytesCounter, blobsCounter);
            }
        }
    }
}
