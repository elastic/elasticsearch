/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Set;

import static java.nio.file.StandardOpenOption.READ;

class MemorySegmentUtils {

    private static final Logger log = LogManager.getLogger(MemorySegmentUtils.class);

    /**
     * Encapsulates a {@link MemorySegment} in a closeable interface; when the MemorySegment is no longer used,
     * call {@link Closeable#close()} to release all the resources associated with the {@link MemorySegment}.
     */
    public interface MemorySegmentHolder extends Closeable {
        MemorySegment memorySegment();

        @Override
        default void close() {}
    }

    private MemorySegmentUtils() {}

    /**
     * Creates a file-backed MemorySegment, mapping the first {@param dataSize} bytes from {@param dataFile}, using the
     * Java {@link FileChannel} API.
     */
    static MemorySegmentHolder createFileBackedMemorySegment(Path dataFile, long dataSize) throws IOException {
        Arena arena = null;
        try {
            arena = Arena.ofConfined();
            try (FileChannel fc = FileChannel.open(dataFile, Set.of(READ))) {
                MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, dataSize, arena);
                return new FileBackedMemorySegmentHolder(mapped, arena, dataFile);
            }
        } catch (Throwable t) {
            if (arena != null) {
                arena.close();
            }
            IOUtils.deleteFilesIgnoringExceptions(dataFile);
            throw t;
        }
    }

    /**
     * This method returns a contiguous {@link MemorySegment} over the whole vector data.
     * <p>
     * Native methods accepting a single memory pointer (e.g. cuVS functions) expect all data to be accessible as a contiguous block.
     * This method tries to get a {@link MemorySegment} slice for the whole {@link IndexInput}. If the input
     * is larger than {@link MMapDirectory#getMaxChunkSize()} (default {@link MMapDirectory#DEFAULT_MAX_CHUNK_SIZE}),
     * this fails, as {@link MMapDirectory} divides it into multiple {@link MemorySegment}s.
     * In this case, the method falls back to creating a file-backed {@link MemorySegment} over the whole vector data.
     * The file-backed MemorySegment is created by copying all the vector data to a temporary file, then manually mapping the whole  file
     * via the Java {@link FileChannel} API.
     */
    public static MemorySegmentHolder getContiguousMemorySegment(MemorySegmentAccessInput input, Directory dir, String baseName)
        throws IOException {
        var inputSlice = input.segmentSliceOrNull(0L, input.length());
        if (inputSlice != null) {
            return new DirectMemorySegmentHolder(inputSlice);
        }

        // The only implementation of MemorySegmentAccessInput is MemorySegmentIndexInput, which is currently used only by
        // MMapDirectory. Other implementations are unlikely but theoretically possible, so better assert so we have the
        // opportunity to catch this in CI, if that ever happens.
        assert dir instanceof FSDirectory;

        log.info(
            "Unable to get a contiguous memory segment for [{}, size{}]. Falling back to manual mapping a temp copy.",
            baseName,
            input.length()
        );
        Path tempVectorsFilePath = copyInputToTempFile((IndexInput) input, (FSDirectory) dir, baseName);
        return createFileBackedMemorySegment(tempVectorsFilePath, input.length());
    }

    /**
     * This method returns a contiguous {@link MemorySegment} over the whole vector data.
     * <p>
     * Native methods accepting a single memory pointer (e.g. cuVS functions) expect all data to be accessible as a contiguous block;
     * however some vector formats add padding and/or extra data (e.g. corrections) after the vector values.
     * Before passing a {@link MemorySegment}/memory pointer to these data, we need to compact them.
     * This method employs a similar schema to {@link MemorySegmentUtils#getContiguousMemorySegment}, using a single MemorySegment from
     * the input {@link MemorySegmentAccessInput} when possible, and falling back to a file-backed {@link MemorySegment} when the
     * input data is too large.
     */
    public static MemorySegmentHolder getContiguousPackedMemorySegment(
        MemorySegmentAccessInput input,
        Directory dir,
        String baseName,
        int numVectors,
        int sourceRowPitch,
        int packedRowSize
    ) throws IOException {
        long packedVectorsDataSize = (long) numVectors * packedRowSize;
        MemorySegment sourceSegment = input.segmentSliceOrNull(0, input.length());
        if (sourceSegment != null) {
            Arena arena = null;
            try {
                arena = Arena.ofConfined();
                var packedSegment = arena.allocate(packedVectorsDataSize, 64);
                for (int i = 0; i < numVectors; i++) {
                    MemorySegment.copy(sourceSegment, (long) i * sourceRowPitch, packedSegment, (long) i * packedRowSize, packedRowSize);
                }
                return new ArenaMemorySegmentHolder(packedSegment, arena);
            } catch (Throwable t) {
                if (arena != null) {
                    arena.close();
                }
                throw t;
            }
        }

        assert dir instanceof FSDirectory;

        log.info(
            "Unable to get a contiguous memory segment for [{}, size{}]. Falling back creating a packed temp copy.",
            baseName,
            input.length()
        );
        var tempVectorsFile = copyInputToTempFilePacked(
            (IndexInput) input,
            (FSDirectory) dir,
            baseName,
            numVectors,
            sourceRowPitch,
            packedRowSize
        );
        return createFileBackedMemorySegment(tempVectorsFile, packedVectorsDataSize);
    }

    static Path copyInputToTempFile(IndexInput input, FSDirectory dir, String baseName) throws IOException {
        try (
            IndexInput clonedInput = input.clone();
            IndexOutput tempVectorsFile = dir.createTempOutput(baseName, "vec_", IOContext.DEFAULT)
        ) {
            clonedInput.seek(0);
            tempVectorsFile.copyBytes(clonedInput, clonedInput.length());
            return dir.getDirectory().resolve(tempVectorsFile.getName());
        }
    }

    /**
     * Copies {@param input} vector data to a temporary file, packing the vectors to remove any padding at the end of the vector data.
     * @param input             the file holding the vector data
     * @param dir               the Lucene data directory
     * @param baseName          the input name
     * @param numVectors        the number of vectors in the input file
     * @param sourceRowPitch    the distance (in bytes) between 2 vectors in the input file, extra data/padding included
     * @param packedRowSize     the size (in bytes) of a single vector
     * @return the absolute path of the newly created temp file.
     */
    static Path copyInputToTempFilePacked(
        IndexInput input,
        FSDirectory dir,
        String baseName,
        int numVectors,
        int sourceRowPitch,
        int packedRowSize
    ) throws IOException {
        try (
            IndexInput clonedInput = input.clone();
            IndexOutput tempVectorsFile = dir.createTempOutput(baseName, "vec_", IOContext.DEFAULT)
        ) {

            byte[] buffer = new byte[packedRowSize];
            for (int i = 0; i < numVectors; i++) {
                clonedInput.seek((long) i * sourceRowPitch);
                clonedInput.readBytes(buffer, 0, packedRowSize);
                tempVectorsFile.writeBytes(buffer, 0, packedRowSize);
            }

            return dir.getDirectory().resolve(tempVectorsFile.getName());
        }
    }

    record FileBackedMemorySegmentHolder(MemorySegment memorySegment, Arena arena, Path dataFile) implements MemorySegmentHolder {
        @Override
        public void close() {
            arena.close();
            IOUtils.deleteFilesIgnoringExceptions(dataFile);
        }
    }

    record ArenaMemorySegmentHolder(MemorySegment memorySegment, Arena arena) implements MemorySegmentHolder {
        @Override
        public void close() {
            arena.close();
        }
    }

    record DirectMemorySegmentHolder(MemorySegment memorySegment) implements MemorySegmentHolder {}
}
