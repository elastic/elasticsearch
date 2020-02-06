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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultiFileWriter extends AbstractRefCounted implements Releasable {

    public MultiFileWriter(Store store, RecoveryState.Index indexState, String tempFilePrefix, Logger logger, Runnable ensureOpen) {
        super("multi_file_writer");
        this.store = store;
        this.indexState = indexState;
        this.tempFilePrefix = tempFilePrefix;
        this.logger = logger;
        this.ensureOpen = ensureOpen;
    }

    private final Runnable ensureOpen;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Logger logger;
    private final Store store;
    private final RecoveryState.Index indexState;
    private final String tempFilePrefix;

    private final ConcurrentMap<String, IndexOutput> openIndexOutputs = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<String, FileChunkWriter> fileChunkWriters = ConcurrentCollections.newConcurrentMap();


    final Map<String, String> tempFileNames = ConcurrentCollections.newConcurrentMap();

    public void writeFileChunk(StoreFileMetaData fileMetaData, long position, BytesReference content, boolean lastChunk)
        throws IOException {
        assert Transports.assertNotTransportThread("multi_file_writer");
        final FileChunkWriter writer = fileChunkWriters.computeIfAbsent(fileMetaData.name(), name -> new FileChunkWriter());
        writer.writeChunk(new FileChunk(fileMetaData, content, position, lastChunk));
    }

    /** Get a temporary name for the provided file name. */
    String getTempNameForFile(String origFile) {
        return tempFilePrefix + origFile;
    }

    public IndexOutput getOpenIndexOutput(String key) {
        ensureOpen.run();
        return openIndexOutputs.get(key);
    }

    /** remove and {@link IndexOutput} for a given file. It is the caller's responsibility to close it */
    public IndexOutput removeOpenIndexOutputs(String name) {
        ensureOpen.run();
        return openIndexOutputs.remove(name);
    }

    /**
     * Creates an {@link IndexOutput} for the given file name. Note that the
     * IndexOutput actually point at a temporary file.
     * <p>
     * Note: You can use {@link #getOpenIndexOutput(String)} with the same filename to retrieve the same IndexOutput
     * at a later stage
     */
    public IndexOutput openAndPutIndexOutput(String fileName, StoreFileMetaData metaData, Store store) throws IOException {
        ensureOpen.run();
        String tempFileName = getTempNameForFile(fileName);
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        // add first, before it's created
        tempFileNames.put(tempFileName, fileName);
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, metaData, IOContext.DEFAULT);
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    private void innerWriteFileChunk(StoreFileMetaData fileMetaData, long position,
                                     BytesReference content, boolean lastChunk) throws IOException {
        final String name = fileMetaData.name();
        IndexOutput indexOutput;
        if (position == 0) {
            indexOutput = openAndPutIndexOutput(name, fileMetaData, store);
        } else {
            indexOutput = getOpenIndexOutput(name);
        }
        assert indexOutput.getFilePointer() == position : "file-pointer " + indexOutput.getFilePointer() + " != " + position;
        BytesRefIterator iterator = content.iterator();
        BytesRef scratch;
        while((scratch = iterator.next()) != null) { // we iterate over all pages - this is a 0-copy for all core impls
            indexOutput.writeBytes(scratch.bytes, scratch.offset, scratch.length);
        }
        indexState.addRecoveredBytesToFile(name, content.length());
        if (indexOutput.getFilePointer() >= fileMetaData.length() || lastChunk) {
            try {
                Store.verify(indexOutput);
            } finally {
                // we are done
                indexOutput.close();
            }
            final String temporaryFileName = getTempNameForFile(name);
            assert Arrays.asList(store.directory().listAll()).contains(temporaryFileName) :
                "expected: [" + temporaryFileName + "] in " + Arrays.toString(store.directory().listAll());
            store.directory().sync(Collections.singleton(temporaryFileName));
            IndexOutput remove = removeOpenIndexOutputs(name);
            assert remove == null || remove == indexOutput; // remove maybe null if we got finished
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            decRef();
        }
    }

    @Override
    protected void closeInternal() {
        fileChunkWriters.clear();
        // clean open index outputs
        Iterator<Map.Entry<String, IndexOutput>> iterator = openIndexOutputs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, IndexOutput> entry = iterator.next();
            logger.trace("closing IndexOutput file [{}]", entry.getValue());
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.debug(() -> new ParameterizedMessage("error while closing recovery output [{}]", entry.getValue()), e);
            }
            iterator.remove();
        }
        if (Strings.hasText(tempFilePrefix)) {
            // trash temporary files
            for (String file : tempFileNames.keySet()) {
                logger.trace("cleaning temporary file [{}]", file);
                store.deleteQuiet(file);
            }
        }
    }

    /** renames all temporary files to their true name, potentially overriding existing files */
    public void renameAllTempFiles() throws IOException {
        ensureOpen.run();
        store.renameTempFilesSafe(tempFileNames);
    }

    static final class FileChunk {
        final StoreFileMetaData md;
        final BytesReference content;
        final long position;
        final boolean lastChunk;
        FileChunk(StoreFileMetaData md, BytesReference content, long position, boolean lastChunk) {
            this.md = md;
            this.content = content;
            this.position = position;
            this.lastChunk = lastChunk;
        }
    }

    private final class FileChunkWriter {
        // chunks can be delivered out of order, we need to buffer chunks if there's a gap between them.
        final PriorityQueue<FileChunk> pendingChunks = new PriorityQueue<>(Comparator.comparing(fc -> fc.position));
        long lastPosition = 0;

        void writeChunk(FileChunk newChunk) throws IOException {
            synchronized (this) {
                pendingChunks.add(newChunk);
            }
            while (true) {
                final FileChunk chunk;
                synchronized (this) {
                    chunk = pendingChunks.peek();
                    if (chunk == null || chunk.position != lastPosition) {
                        return;
                    }
                    pendingChunks.remove();
                }
                innerWriteFileChunk(chunk.md, chunk.position, chunk.content, chunk.lastChunk);
                synchronized (this) {
                    assert lastPosition == chunk.position : "last_position " + lastPosition + " != chunk_position " + chunk.position;
                    lastPosition += chunk.content.length();
                    if (chunk.lastChunk) {
                        assert pendingChunks.isEmpty() : "still have pending chunks [" + pendingChunks + "]";
                        fileChunkWriters.remove(chunk.md.name());
                        assert fileChunkWriters.containsValue(this) == false : "chunk writer [" + newChunk.md + "] was not removed";
                    }
                }
            }
        }
    }
}
