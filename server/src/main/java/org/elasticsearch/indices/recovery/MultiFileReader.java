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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;

/**
 * A thread safe implementation that allows reading multiple files sequentially chunk by chunk.
 */
final class MultiFileReader implements Closeable {
    private boolean closed = false;
    private final Store store;
    private final Iterator<StoreFileMetaData> remainingFiles;
    private StoreFileMetaData md;
    private InputStreamIndexInput currentInput = null;
    private long currentChunkPosition = 0;
    private final int chunkSizeInBytes;
    private final Deque<byte[]> recycledBuffers;

    MultiFileReader(Store store, StoreFileMetaData[] files, int chunkSizeInBytes) {
        this.store = store;
        this.remainingFiles = Arrays.asList(files).iterator();
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.recycledBuffers = ConcurrentCollections.newDeque();
    }

    synchronized StoreFileMetaData currentFile() {
        assert md != null;
        return md;
    }

    /**
     * Reads the next file chunk if available. This method returns {@code null} when all provided files are exhaustedly read.
     * When the caller no longer needs the returned file chunk, it should call {@link FileChunk#close()} to release that file
     * chunk so the reader can reuse the associated buffer to reduce allocation.
     */
    synchronized FileChunk readNextChunk() throws IOException {
        if (closed) {
            throw new IllegalStateException("MultiFileReader was closed");
        }
        if (currentInput == null) {
            if (remainingFiles.hasNext() == false) {
                return null;
            }
            currentChunkPosition = 0;
            md = remainingFiles.next();
            final IndexInput indexInput = store.directory().openInput(md.name(), IOContext.READONCE);
            currentInput = new InputStreamIndexInput(indexInput, md.length()) {
                @Override
                public void close() throws IOException {
                    indexInput.close(); //InputStreamIndexInput's close is noop
                }
            };
        }
        final byte[] buffer = Objects.requireNonNullElseGet(recycledBuffers.pollFirst(), () -> new byte[chunkSizeInBytes]);
        final int bytesRead = currentInput.read(buffer);
        if (bytesRead == -1) {
            throw new CorruptIndexException("file truncated, expected length= " + md.length() + " position=" + currentChunkPosition, md.name());
        }
        final long chunkPosition = currentChunkPosition;
        currentChunkPosition += bytesRead;
        final boolean lastChunk = currentChunkPosition == md.length();
        final FileChunk chunk = new FileChunk(
            md, new BytesArray(buffer, 0, bytesRead), chunkPosition, lastChunk, () -> recycledBuffers.addFirst(buffer));
        if (lastChunk) {
            IOUtils.close(currentInput, () -> currentInput = null);
        }
        return chunk;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed == false) {
            closed = true;
            IOUtils.close(currentInput, () -> currentInput = null);
        }
    }

    static final class FileChunk implements Releasable {
        final StoreFileMetaData md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;
        private final Releasable onClose;

        private FileChunk(StoreFileMetaData md, BytesReference content, long position, boolean lastChunk, Releasable onClose) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
            this.onClose = onClose;
        }

        @Override
        public void close() {
            onClose.close();
        }
    }
}
