/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.transport.Transports;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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

    public void writeFileChunk(StoreFileMetadata fileMetadata, long position, ReleasableBytesReference content, boolean lastChunk)
        throws IOException {
        assert Transports.assertNotTransportThread("multi_file_writer");
        final FileChunkWriter writer = fileChunkWriters.computeIfAbsent(fileMetadata.name(), name -> new FileChunkWriter());
        incRef();
        try {
            writer.writeChunk(new FileChunk(fileMetadata, content, position, lastChunk));
        } finally {
            decRef();
        }
    }

    public void writeFile(StoreFileMetadata fileMetadata, ByteSizeValue readSnapshotFileBufferSize,  InputStream stream) throws Exception {
        ensureOpen.run();
        assert Transports.assertNotTransportThread("multi_file_writer");

        String fileName = fileMetadata.name();
        String tempFileName = getTempNameForFile(fileName);
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        tempFileNames.put(tempFileName, fileName);

        incRef();
        try(IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, fileMetadata, IOContext.DEFAULT)) {
            int bufferSize = Math.toIntExact(Math.min(readSnapshotFileBufferSize.getBytes(), fileMetadata.length()));
            byte[] buffer = new byte[bufferSize];
            int length;
            long bytesWritten = 0;
            while ((length = stream.read(buffer)) > 0) {
                indexOutput.writeBytes(buffer, length);
                indexState.addRecoveredBytesToFile(fileName, length);
                bytesWritten += length;
            }

            if (bytesWritten < fileMetadata.length()) {
                throw new EOFException("Expected to write a file of length [" + fileMetadata.length() + "] " +
                    "but only [" + bytesWritten + "] bytes were written");
            }

            Store.verify(indexOutput);
            assert Arrays.asList(store.directory().listAll()).contains(tempFileName) :
                "expected: [" + tempFileName + "] in " + Arrays.toString(store.directory().listAll());
            store.directory().sync(Collections.singleton(tempFileName));
        } catch (Exception e) {
            tempFileNames.remove(tempFileName);
            store.deleteQuiet(tempFileName);
            indexState.resetRecoveredBytesOfFile(fileName);
            throw e;
        } finally {
            decRef();
        }
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
    public IndexOutput openAndPutIndexOutput(String fileName, StoreFileMetadata metadata, Store store) throws IOException {
        ensureOpen.run();
        String tempFileName = getTempNameForFile(fileName);
        if (tempFileNames.containsKey(tempFileName)) {
            throw new IllegalStateException("output for file [" + fileName + "] has already been created");
        }
        // add first, before it's created
        tempFileNames.put(tempFileName, fileName);
        IndexOutput indexOutput = store.createVerifyingOutput(tempFileName, metadata, IOContext.DEFAULT);
        openIndexOutputs.put(fileName, indexOutput);
        return indexOutput;
    }

    private void innerWriteFileChunk(StoreFileMetadata fileMetadata, long position,
                                     BytesReference content, boolean lastChunk) throws IOException {
        final String name = fileMetadata.name();
        IndexOutput indexOutput;
        if (position == 0) {
            indexOutput = openAndPutIndexOutput(name, fileMetadata, store);
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
        if (indexOutput.getFilePointer() >= fileMetadata.length() || lastChunk) {
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
        Releasables.close(fileChunkWriters.values());
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

    private static final class FileChunk implements Releasable {
        final StoreFileMetadata md;
        final ReleasableBytesReference content;
        final long position;
        final boolean lastChunk;
        FileChunk(StoreFileMetadata md, ReleasableBytesReference content, long position, boolean lastChunk) {
            this.md = md;
            this.content = content.retain();
            this.position = position;
            this.lastChunk = lastChunk;
        }

        @Override
        public void close() {
            content.decRef();
        }
    }

    private final class FileChunkWriter implements Releasable {
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
                try (chunk) {
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

        @Override
        public synchronized void close() {
            Releasables.close(pendingChunks);
        }
    }
}
