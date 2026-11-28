/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.List;

/**
 * An Iceberg {@link FileIO} over the blob store of a snapshot repository, so that Iceberg writes and reads its metadata with the
 * repository and its credentials, and our plugin needs no S3 client of its own.
 *
 * <p>The locations that Iceberg uses are the URIs that end up in the table metadata, for example {@code s3://bucket/base/metadata/v1.json}.
 * The URI of the table root ({@code rootLocation}) maps to {@code basePath} in the blob store, and the rest of a location is the path
 * below it. Readers outside Elasticsearch need the real URI in the metadata, which is why the root is a parameter and not derived.
 *
 * <p>This class is for the metadata files of a table: manifests, manifest lists and metadata files. They are small, so an input file
 * reads its whole blob on first use and an output file buffers its bytes, and an output file writes with an atomic write so that a
 * reader never sees a partial file. The data files are written and read by the engine and the compactor with the blob container directly
 * (ranged reads, streamed uploads).
 */
public final class RepositoryFileIO implements FileIO {

    public static final OperationPurpose PURPOSE = OperationPurpose.INDICES;

    private final transient BlobStore blobStore;
    private final transient BlobPath basePath;
    private final String rootLocation;

    /**
     * @param blobStore    the blob store of the repository
     * @param basePath     the blob path that the table root maps to
     * @param rootLocation the URI of the table root, without a trailing slash
     */
    public RepositoryFileIO(BlobStore blobStore, BlobPath basePath, String rootLocation) {
        this.blobStore = blobStore;
        this.basePath = basePath;
        this.rootLocation = rootLocation.endsWith("/") ? rootLocation.substring(0, rootLocation.length() - 1) : rootLocation;
    }

    public String rootLocation() {
        return rootLocation;
    }

    /** The container that holds the blob of {@code location}. */
    BlobContainer containerOf(String location) {
        final String relative = relative(location);
        BlobPath path = basePath;
        final String[] parts = relative.split("/");
        for (int i = 0; i < parts.length - 1; i++) {
            path = path.add(parts[i]);
        }
        return blobStore.blobContainer(path);
    }

    /** The name of the blob of {@code location} in its container. */
    String blobNameOf(String location) {
        final String relative = relative(location);
        return relative.substring(relative.lastIndexOf('/') + 1);
    }

    private String relative(String location) {
        if (location.startsWith(rootLocation + "/") == false) {
            throw new IllegalArgumentException("location [" + location + "] is not below the table root [" + rootLocation + "]");
        }
        return location.substring(rootLocation.length() + 1);
    }

    @Override
    public InputFile newInputFile(String location) {
        return new RepositoryInputFile(this, location);
    }

    @Override
    public OutputFile newOutputFile(String location) {
        return new RepositoryOutputFile(this, location);
    }

    @Override
    public void deleteFile(String location) {
        try {
            containerOf(location).deleteBlobsIgnoringIfNotExists(PURPOSE, List.of(blobNameOf(location)).iterator());
        } catch (IOException e) {
            throw new RuntimeIOException(e, "failed to delete [%s]", location);
        }
    }

    @Override
    public void close() {}

    private static final class RepositoryInputFile implements InputFile {
        private final RepositoryFileIO io;
        private final String location;
        private byte[] bytes;

        RepositoryInputFile(RepositoryFileIO io, String location) {
            this.io = io;
            this.location = location;
        }

        private byte[] bytes() {
            if (bytes == null) {
                try (InputStream in = io.containerOf(location).readBlob(PURPOSE, io.blobNameOf(location))) {
                    bytes = in.readAllBytes();
                } catch (NoSuchFileException e) {
                    throw new NotFoundException(e, "location [%s] does not exist", location);
                } catch (IOException e) {
                    throw new RuntimeIOException(e, "failed to read [%s]", location);
                }
            }
            return bytes;
        }

        @Override
        public long getLength() {
            return bytes().length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new BytesSeekableInputStream(bytes());
        }

        @Override
        public String location() {
            return location;
        }

        @Override
        public boolean exists() {
            try {
                return io.containerOf(location).blobExists(PURPOSE, io.blobNameOf(location));
            } catch (IOException e) {
                throw new RuntimeIOException(e, "failed to check [%s]", location);
            }
        }
    }

    private static final class BytesSeekableInputStream extends SeekableInputStream {
        private final byte[] bytes;
        private int position;

        BytesSeekableInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < 0 || newPos > bytes.length) {
                throw new EOFException("cannot seek to [" + newPos + "] in a file of [" + bytes.length + "] bytes");
            }
            position = (int) newPos;
        }

        @Override
        public int read() {
            return position < bytes.length ? bytes[position++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] destination, int offset, int length) {
            if (length == 0) {
                return 0;
            }
            if (position >= bytes.length) {
                return -1;
            }
            final int count = Math.min(length, bytes.length - position);
            System.arraycopy(bytes, position, destination, offset, count);
            position += count;
            return count;
        }
    }

    private static final class RepositoryOutputFile implements OutputFile {
        private final RepositoryFileIO io;
        private final String location;

        RepositoryOutputFile(RepositoryFileIO io, String location) {
            this.io = io;
            this.location = location;
        }

        @Override
        public PositionOutputStream create() {
            try {
                if (io.containerOf(location).blobExists(PURPOSE, io.blobNameOf(location))) {
                    throw new AlreadyExistsException("location [%s] already exists", location);
                }
            } catch (IOException e) {
                throw new RuntimeIOException(e, "failed to check [%s]", location);
            }
            return new BufferedOutputStream(true);
        }

        @Override
        public PositionOutputStream createOrOverwrite() {
            return new BufferedOutputStream(false);
        }

        @Override
        public String location() {
            return location;
        }

        @Override
        public InputFile toInputFile() {
            return new RepositoryInputFile(io, location);
        }

        /** Buffers the bytes, and writes the blob atomically when the stream is closed. */
        private final class BufferedOutputStream extends PositionOutputStream {
            private final boolean failIfAlreadyExists;
            private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            private boolean closed;

            BufferedOutputStream(boolean failIfAlreadyExists) {
                this.failIfAlreadyExists = failIfAlreadyExists;
            }

            @Override
            public long getPos() {
                return buffer.size();
            }

            @Override
            public void write(int b) {
                buffer.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                buffer.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    io.containerOf(location)
                        .writeBlobAtomic(PURPOSE, io.blobNameOf(location), new BytesArray(buffer.toByteArray()), failIfAlreadyExists);
                } catch (FileAlreadyExistsException e) {
                    throw new AlreadyExistsException("location [%s] already exists", location);
                }
            }
        }
    }
}
