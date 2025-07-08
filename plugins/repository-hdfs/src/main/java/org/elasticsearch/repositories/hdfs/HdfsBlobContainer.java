/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.hdfs.HdfsBlobStore.Operation;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

final class HdfsBlobContainer extends AbstractBlobContainer {
    private final HdfsBlobStore store;
    private final HdfsSecurityContext securityContext;
    private final Path path;
    private final int bufferSize;

    private final Short replicationFactor;
    private final Options.CreateOpts[] createOpts;

    HdfsBlobContainer(
        BlobPath blobPath,
        HdfsBlobStore store,
        Path path,
        int bufferSize,
        HdfsSecurityContext hdfsSecurityContext,
        Short replicationFactor
    ) {
        super(blobPath);
        this.store = store;
        this.securityContext = hdfsSecurityContext;
        this.path = path;
        this.bufferSize = bufferSize;
        this.replicationFactor = replicationFactor;
        this.createOpts = replicationFactor == null ? new CreateOpts[0] : new CreateOpts[] { CreateOpts.repFac(replicationFactor) };
    }

    // TODO: See if we can get precise result reporting.
    private static final DeleteResult DELETE_RESULT = new DeleteResult(1L, 0L);

    @Override
    public boolean blobExists(OperationPurpose purpose, String blobName) throws IOException {
        return store.execute(fileContext -> fileContext.util().exists(new Path(path, blobName)));
    }

    @Override
    public DeleteResult delete(OperationPurpose purpose) throws IOException {
        store.execute(fileContext -> fileContext.delete(path, true));
        return DELETE_RESULT;
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, final Iterator<String> blobNames) throws IOException {
        IOException ioe = null;
        while (blobNames.hasNext()) {
            final String blobName = blobNames.next();
            try {
                store.execute(fileContext -> fileContext.delete(new Path(path, blobName), true));
            } catch (final FileNotFoundException ignored) {
                // This exception is ignored
            } catch (IOException e) {
                if (ioe == null) {
                    ioe = e;
                } else {
                    ioe.addSuppressed(e);
                }
            }
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
        // FSDataInputStream does buffering internally
        // FSDataInputStream can open connections on read() or skip() so we wrap in
        // HDFSPrivilegedInputSteam which will ensure that underlying methods will
        // be called with the proper privileges.
        try {
            return store.execute(fileContext -> fileContext.open(new Path(path, blobName), bufferSize));
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
        // FSDataInputStream does buffering internally
        // FSDataInputStream can open connections on read() or skip() so we wrap in
        // HDFSPrivilegedInputSteam which will ensure that underlying methods will
        // be called with the proper privileges.
        try {
            return store.execute(fileContext -> {
                FSDataInputStream fsInput = fileContext.open(new Path(path, blobName), bufferSize);
                // As long as no read operations have happened yet on the stream, seeking
                // should direct the datanode to start on the appropriate block, at the
                // appropriate target position.
                fsInput.seek(position);
                return Streams.limitStream(fsInput, length);
            });
        } catch (FileNotFoundException fnfe) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        }
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        Path blob = new Path(path, blobName);
        // we pass CREATE, which means it fails if a blob already exists.
        final EnumSet<CreateFlag> flags = failIfAlreadyExists
            ? EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK)
            : EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE, CreateFlag.SYNC_BLOCK);
        store.execute((Operation<Void>) fileContext -> {
            try {
                writeToPath(inputStream, blobSize, fileContext, blob, flags);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        Path blob = new Path(path, blobName);
        // we pass CREATE, which means it fails if a blob already exists.
        final EnumSet<CreateFlag> flags = failIfAlreadyExists
            ? EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK)
            : EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE, CreateFlag.SYNC_BLOCK);
        store.execute((Operation<Void>) fileContext -> {
            try {
                writeToPath(bytes, blob, fileContext, flags);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    @Override
    public void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        Path blob = new Path(path, blobName);
        if (atomic) {
            final Path tempBlobPath = new Path(path, FsBlobContainer.tempBlobName(blobName));
            store.execute((Operation<Void>) fileContext -> {
                try {
                    try (
                        FSDataOutputStream stream = fileContext.create(
                            tempBlobPath,
                            EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK),
                            createOpts
                        )
                    ) {
                        writer.accept(stream);
                    }
                    // Ensure that the stream is closed before renaming so all pending writes are flushed
                    fileContext.rename(tempBlobPath, blob, failIfAlreadyExists ? Options.Rename.NONE : Options.Rename.OVERWRITE);
                } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                    throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
                }
                return null;
            });
        } else {
            // we pass CREATE, which means it fails if a blob already exists.
            final EnumSet<CreateFlag> flags = failIfAlreadyExists
                ? EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK)
                : EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE, CreateFlag.SYNC_BLOCK);
            store.execute((Operation<Void>) fileContext -> {
                try (FSDataOutputStream stream = fileContext.create(blob, flags, createOpts)) {
                    writer.accept(stream);
                } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                    throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
                }
                return null;
            });
        }
    }

    @Override
    public void writeBlobAtomic(
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        final String tempBlob = FsBlobContainer.tempBlobName(blobName);
        final Path tempBlobPath = new Path(path, tempBlob);
        final Path blob = new Path(path, blobName);
        store.execute((Operation<Void>) fileContext -> {
            writeToPath(inputStream, blobSize, fileContext, tempBlobPath, EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK));
            try {
                fileContext.rename(tempBlobPath, blob, failIfAlreadyExists ? Options.Rename.NONE : Options.Rename.OVERWRITE);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    @Override
    public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
        throws IOException {
        final String tempBlob = FsBlobContainer.tempBlobName(blobName);
        final Path tempBlobPath = new Path(path, tempBlob);
        final Path blob = new Path(path, blobName);
        store.execute((Operation<Void>) fileContext -> {
            writeToPath(bytes, tempBlobPath, fileContext, EnumSet.of(CreateFlag.CREATE, CreateFlag.SYNC_BLOCK));
            try {
                fileContext.rename(tempBlobPath, blob, failIfAlreadyExists ? Options.Rename.NONE : Options.Rename.OVERWRITE);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException faee) {
                throw new FileAlreadyExistsException(blob.toString(), null, faee.getMessage());
            }
            return null;
        });
    }

    private void writeToPath(BytesReference bytes, Path blobPath, FileContext fileContext, EnumSet<CreateFlag> createFlags)
        throws IOException {
        try (FSDataOutputStream stream = fileContext.create(blobPath, createFlags, createOpts)) {
            bytes.writeTo(stream);
        }
    }

    private void writeToPath(
        InputStream inputStream,
        long blobSize,
        FileContext fileContext,
        Path blobPath,
        EnumSet<CreateFlag> createFlags
    ) throws IOException {
        final byte[] buffer = new byte[blobSize < bufferSize ? Math.toIntExact(blobSize) : bufferSize];

        Options.CreateOpts[] createOptsWithBufferSize = addOptionToArray(createOpts, CreateOpts.bufferSize(buffer.length));
        try (FSDataOutputStream stream = fileContext.create(blobPath, createFlags, createOptsWithBufferSize)) {
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                stream.write(buffer, 0, bytesRead);
            }
            assert stream.size() == blobSize : "Expected to write [" + blobSize + "] bytes but wrote [" + stream.size() + "] bytes";
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, @Nullable final String prefix) throws IOException {
        FileStatus[] files;
        try {
            files = store.execute(
                fileContext -> fileContext.util().listStatus(path, eachPath -> prefix == null || eachPath.getName().startsWith(prefix))
            );
        } catch (FileNotFoundException e) {
            files = new FileStatus[0];
        }
        Map<String, BlobMetadata> map = new LinkedHashMap<>();
        for (FileStatus file : files) {
            if (file.isFile()) {
                map.put(file.getPath().getName(), new BlobMetadata(file.getPath().getName(), file.getLen()));
            }
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return listBlobsByPrefix(purpose, null);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        FileStatus[] files = store.execute(fileContext -> fileContext.util().listStatus(path));
        Map<String, BlobContainer> map = new LinkedHashMap<>();
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                final String name = file.getPath().getName();
                map.put(
                    name,
                    new HdfsBlobContainer(path().add(name), store, new Path(path, name), bufferSize, securityContext, replicationFactor)
                );
            }
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public void compareAndExchangeRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        listener.onFailure(new UnsupportedOperationException("HDFS repositories do not support this operation"));
    }

    private static CreateOpts[] addOptionToArray(final CreateOpts[] opts, final CreateOpts opt) {
        if (opts == null) {
            return new CreateOpts[] { opt };
        }
        CreateOpts[] newOpts = new CreateOpts[opts.length + 1];
        System.arraycopy(opts, 0, newOpts, 0, opts.length);
        newOpts[opts.length] = opt;

        return newOpts;
    }
}
