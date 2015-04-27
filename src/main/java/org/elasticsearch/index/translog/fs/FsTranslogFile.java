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

package org.elasticsearch.index.translog.fs;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.index.translog.TranslogStream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public interface FsTranslogFile extends Closeable {

    public static enum Type {

        SIMPLE() {
            @Override
            public FsTranslogFile create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new SimpleFsTranslogFile(shardId, id, channelReference);
            }
        },
        BUFFERED() {
            @Override
            public FsTranslogFile create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize) throws IOException {
                return new BufferingFsTranslogFile(shardId, id, channelReference, bufferSize);
            }
        };

        public abstract FsTranslogFile create(ShardId shardId, long id, ChannelReference raf, int bufferSize) throws IOException;

        public static Type fromString(String type) throws ElasticsearchIllegalArgumentException {
            if (SIMPLE.name().equalsIgnoreCase(type)) {
                return SIMPLE;
            } else if (BUFFERED.name().equalsIgnoreCase(type)) {
                return BUFFERED;
            }
            throw new ElasticsearchIllegalArgumentException("No translog fs type [" + type + "]");
        }
    }

    long id();

    int estimatedNumberOfOperations();

    long translogSizeInBytes();

    Translog.Location add(BytesReference data) throws IOException;

    byte[] read(Translog.Location location) throws IOException;

    FsChannelSnapshot snapshot() throws TranslogException;

    void reuse(FsTranslogFile other) throws TranslogException;

    void updateBufferSize(int bufferSize) throws TranslogException;

    void sync() throws IOException;

    boolean syncNeeded();

    TranslogStream getStream();

    public Path getPath();

    public boolean closed();
}
