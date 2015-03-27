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
            final Map<FsChannelReader, RuntimeException> existing = openedFiles.put(file().toString(), ConcurrentCollections.<FsChannelReader, RuntimeException>newConcurrentMap());
            assert existing == null || existing.size() == 0 : "a channel for the file[" + file + "] was previously opened from " + ExceptionsHelper.stackTrace(Iterables.getFirst(existing.values(), null));
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

    /**
     * called to add this owner to the list of reference holders (used for leakage detection).
     * also asserts that there is no double "attachment"
     */
    boolean assertAttach(FsChannelReader owner) {
        Map<FsChannelReader, RuntimeException> ownerMap = openedFiles.get(file().toString());
        Throwable previous = ownerMap.put(owner, new RuntimeException(file.toString() + " attached", null));
        assert previous == null : "double attachment by the same owner";
        return true;
    }

    /** removes an owner to the least of list holders (used for leakage detection).
     * also asserts that this owner did attach before.
     */
    boolean assertDetach(FsChannelReader owner) {
        Map<FsChannelReader, RuntimeException> ownerMap = openedFiles.get(file().toString());
        Throwable previous = ownerMap.remove(owner);
        assert previous != null : "reader detaches, but was never attached";
        return true;
    }

    @Override
    public String toString() {
        return "channel: file [" + file + "], ref count [" + refCount() + "]";
    }

    @Override
    protected void closeInternal() {
        IOUtils.closeWhileHandlingException(channel);
        assert openedFiles.remove(file().toString()) != null;
    }

    // per file, which objects refer to it and a throwable of the allocation code
    static final Map<String, Map<FsChannelReader, RuntimeException>> openedFiles;

    static {
        boolean assertsEnabled = false;
        assert (assertsEnabled = true);
        if (assertsEnabled) {
            openedFiles = ConcurrentCollections.newConcurrentMap();
        } else {
            openedFiles = null;
        }
    }


}
