/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.index.IndexSettings;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_DIRECT_IO_DEFAULT_MERGE_BUFFER_SIZE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_DIRECT_IO_DEFAULT_MIN_BYTES_DIRECT_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_USE_DIRECT_IO_FOR_MERGES_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_USE_DIRECT_IO_FOR_SNAPSHOTS_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_IO_CONTEXT;

public class ESDirectIODirectory extends FilterDirectory {

    final boolean useDirectIOForMerges;
    final boolean useDirectIOForSnapshots;
    final long minBytesDirect;

    /**
     * Create a new ESDirectIODirectory for the named location.
     *
     * @param delegate Directory for non-merges, also used as reference to file system path.
     * @param indexSettings the index settings
     * @throws IOException If there is a low-level I/O error
     */
    public ESDirectIODirectory(FSDirectory delegate, IndexSettings indexSettings) throws IOException {
        super(delegate);
        this.mergeBufferSize = Math.toIntExact(indexSettings.getValue(INDEX_STORE_DIRECT_IO_DEFAULT_MERGE_BUFFER_SIZE_SETTING).getBytes());
        this.minBytesDirect = indexSettings.getValue(INDEX_STORE_DIRECT_IO_DEFAULT_MIN_BYTES_DIRECT_SETTING).getBytes();
        this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
        useDirectIOForMerges = indexSettings.getValue(INDEX_STORE_USE_DIRECT_IO_FOR_MERGES_SETTING);
        useDirectIOForSnapshots = indexSettings.getValue(INDEX_STORE_USE_DIRECT_IO_FOR_SNAPSHOTS_SETTING);
    }

    /**
     * Determines if direct IO should be used for a file. By default this tests if it is a merge
     * context and if the merge or file length extends the minimum size (see {@link
     * #DEFAULT_MIN_BYTES_DIRECT}). Subclasses may override method to enforce direct IO for specific
     * file types.
     *
     * @param name file name (unused by default implementation)
     * @param context information about merge size
     * @param fileLength if available, gives the file length. Will be empty when requesting an {@link
     *     IndexOutput}.
     * @return {@code true} if direct IO should be used; {@code false} if input/output should be
     *     requested from delegate directory.
     */
    protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
        if (useDirectIOForMerges && context.context == IOContext.Context.MERGE) {
            return context.context == IOContext.Context.MERGE
                && context.mergeInfo.estimatedMergeBytes >= minBytesDirect
                && fileLength.orElse(minBytesDirect) >= minBytesDirect;
        }
        if (useDirectIOForSnapshots && context == SNAPSHOT_IO_CONTEXT) {
            return fileLength.orElse(minBytesDirect) >= minBytesDirect;
        }
        return false;
    }


    /**
     * Default buffer size before writing to disk (256 KB); larger means less IO load but more RAM and
     * direct buffer storage space consumed during merging.
     */
    public static final int DEFAULT_MERGE_BUFFER_SIZE = 256 * 1024;

    /** Default min expected merge size before direct IO is used (10 MB): */
    public static final long DEFAULT_MIN_BYTES_DIRECT = 10 * 1024 * 1024;

    private final int blockSize, mergeBufferSize;

    volatile boolean isOpen = true;

    /**
     * Reference to {@code com.sun.nio.file.ExtendedOpenOption.DIRECT} by reflective class and enum
     * lookup. There are two reasons for using this instead of directly referencing
     * ExtendedOpenOption.DIRECT:
     *
     * <ol>
     *   <li>ExtendedOpenOption.DIRECT is OpenJDK's internal proprietary API. This API causes
     *       un-suppressible(?) warning to be emitted when compiling with --release flag and value N,
     *       where N is smaller than the the version of javac used for compilation. For details,
     *       please refer to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8259039.
     *   <li>It is possible that Lucene is run using JDK that does not support
     *       ExtendedOpenOption.DIRECT. In such a case, dynamic lookup allows us to bail out with
     *       UnsupportedOperationException with meaningful error message.
     * </ol>
     *
     * <p>This reference is {@code null}, if the JDK does not support direct I/O.
     */
    static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz =
                Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option =
                Arrays.stream(clazz.getEnumConstants())
                    .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
                    .findFirst()
                    .orElse(null);
        } catch (
            @SuppressWarnings("unused")
                Exception e) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    /** @return the underlying file system directory */
    public Path getDirectory() {
        return ((FSDirectory) in).getDirectory();
    }

    @Override
    protected void ensureOpen() throws AlreadyClosedException {
        if (isOpen == false) {
            throw new AlreadyClosedException("this Directory is closed");
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        if (useDirectIO(name, context, OptionalLong.of(fileLength(name)))) {
            return new DirectIOIndexInput(getDirectory().resolve(name), blockSize, mergeBufferSize);
        } else {
            return in.openInput(name, context);
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        if (useDirectIO(name, context, OptionalLong.empty())) {
            return new DirectIOIndexOutput(
                getDirectory().resolve(name), name, blockSize, mergeBufferSize);
        } else {
            return in.createOutput(name, context);
        }
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        super.close();
    }

    private static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
        }
        return ExtendedOpenOption_DIRECT;
    }

    private static final class DirectIOIndexOutput extends IndexOutput {
        private final ByteBuffer buffer;
        private final FileChannel channel;
        private final Checksum digest;

        private long filePos;
        private boolean isOpen;

        /**
         * Creates a new instance of DirectIOIndexOutput for writing index output with direct IO
         * bypassing OS buffer
         *
         * @throws UnsupportedOperationException if the JDK does not support Direct I/O
         * @throws IOException if the operating system or filesystem does not support support Direct I/O
         *     or a sufficient equivalent.
         */
        DirectIOIndexOutput(Path path, String name, int blockSize, int bufferSize)
            throws IOException {
            super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

            channel =
                FileChannel.open(
                    path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW, getDirectOpenOption());
            buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);
            digest = new BufferedChecksum(new CRC32());

            isOpen = true;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            buffer.put(b);
            digest.update(b);
            if (buffer.hasRemaining() == false) {
                dump();
            }
        }

        @Override
        public void writeBytes(byte[] src, int offset, int len) throws IOException {
            int toWrite = len;
            while (true) {
                final int left = buffer.remaining();
                if (left <= toWrite) {
                    buffer.put(src, offset, left);
                    digest.update(src, offset, left);
                    toWrite -= left;
                    offset += left;
                    dump();
                } else {
                    buffer.put(src, offset, toWrite);
                    digest.update(src, offset, toWrite);
                    break;
                }
            }
        }

        private void dump() throws IOException {
            final int size = buffer.position();

            // we need to rewind, as we have to write full blocks (we truncate file later):
            buffer.rewind();

            channel.write(buffer, filePos);
            filePos += size;

            buffer.clear();
        }

        @Override
        public long getFilePointer() {
            return filePos + buffer.position();
        }

        @Override
        public long getChecksum() {
            return digest.getValue();
        }

        @Override
        public void close() throws IOException {
            if (isOpen) {
                isOpen = false;
                try {
                    dump();
                } finally {
                    try (FileChannel ch = channel) {
                        ch.truncate(getFilePointer());
                    }
                }
            }
        }
    }

    private static final class DirectIOIndexInput extends IndexInput {
        private final ByteBuffer buffer;
        private final FileChannel channel;
        private final int blockSize;

        private boolean isOpen;
        private boolean isClone;
        private long filePos;

        /**
         * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing
         * OS buffer
         *
         * @throws UnsupportedOperationException if the JDK does not support Direct I/O
         * @throws IOException if the operating system or filesystem does not support support Direct I/O
         *     or a sufficient equivalent.
         */
        DirectIOIndexInput(Path path, int blockSize, int bufferSize) throws IOException {
            super("DirectIOIndexInput(path=\"" + path + "\")");
            this.blockSize = blockSize;

            this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
            this.buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);

            isOpen = true;
            isClone = false;
            filePos = -bufferSize;
            buffer.limit(0);
        }

        // for clone
        private DirectIOIndexInput(DirectIOIndexInput other) throws IOException {
            super(other.toString());
            this.channel = other.channel;
            this.blockSize = other.blockSize;

            final int bufferSize = other.buffer.capacity();
            this.buffer = ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize);

            isOpen = true;
            isClone = true;
            filePos = -bufferSize;
            buffer.limit(0);
            seek(other.getFilePointer());
        }

        @Override
        public void close() throws IOException {
            if (isOpen && isClone == false) {
                channel.close();
            }
        }

        @Override
        public long getFilePointer() {
            long filePointer = filePos + buffer.position();

            // opening the input and immediately calling getFilePointer without calling readX (and thus
            // refill) first,
            // will result in negative value equal to bufferSize being returned,
            // due to the initialization method filePos = -bufferSize used in constructor.
            assert filePointer == -buffer.capacity() || filePointer >= 0
                : "filePointer should either be initial value equal to negative buffer capacity, or larger than or equal to 0";
            return Math.max(filePointer, 0);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos != getFilePointer()) {
                final long alignedPos = pos - (pos % blockSize);
                filePos = alignedPos - buffer.capacity();

                final int delta = (int) (pos - alignedPos);
                refill(delta);
                buffer.position(delta);
            }
            assert pos == getFilePointer();
        }

        @Override
        public long length() {
            try {
                return channel.size();
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }

        @Override
        public byte readByte() throws IOException {
            if (buffer.hasRemaining() == false) {
                refill(1);
            }

            return buffer.get();
        }

        private void refill(int bytesToRead) throws IOException {
            filePos += buffer.capacity();

            // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
            // hence throwing EOFException early to maintain buffer state (position in particular)
            if (filePos > channel.size() || (channel.size() - filePos < bytesToRead)) {
                throw new EOFException("read past EOF: " + this);
            }

            buffer.clear();
            try {
                // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
                // EOF
                // when filePos > channel.size(), an EOFException will be thrown from above
                channel.read(buffer, filePos);
            } catch (IOException ioe) {
                throw new IOException(ioe.getMessage() + ": " + this, ioe);
            }

            buffer.flip();
        }

        @Override
        public void readBytes(byte[] dst, int offset, int len) throws IOException {
            int toRead = len;
            while (true) {
                final int left = buffer.remaining();
                if (left < toRead) {
                    buffer.get(dst, offset, left);
                    toRead -= left;
                    offset += left;
                    refill(toRead);
                } else {
                    buffer.get(dst, offset, toRead);
                    break;
                }
            }
        }

        @Override
        public DirectIOIndexInput clone() {
            try {
                return new DirectIOIndexInput(this);
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) {
            // TODO: is this the right thing to do?
            return BufferedIndexInput.wrap(sliceDescription, this, offset, length);
        }
    }
}
