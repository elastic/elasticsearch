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

package org.elasticsearch.index.translog;

import com.google.common.collect.Iterables;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.translog.TranslogStream;
import org.elasticsearch.index.translog.TranslogStreams;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

class ChannelReference extends AbstractRefCounted {

    private final Path file;

    private final FileChannel channel;

    private final TranslogStream stream;

    public ChannelReference(Path file, OpenOption... openOptions) throws IOException {
        super(file.toString());
        this.file = file;
        this.channel = FileChannel.open(file, openOptions);
        try {
            this.stream = TranslogStreams.translogStreamFor(file);
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(channel);
            throw t;
        }
    }

    public Path file() {
        return this.file;
    }

    public FileChannel channel() {
        return this.channel;
    }

    public TranslogStream stream() {
        return this.stream;
    }

    @Override
    public String toString() {
        return "channel: file [" + file + "], ref count [" + refCount() + "]";
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(channel);
    }
}
