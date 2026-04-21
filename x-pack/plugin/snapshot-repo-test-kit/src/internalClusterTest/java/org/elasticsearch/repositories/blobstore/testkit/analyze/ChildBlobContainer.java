/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is a stub for child containers returned by the BlobRepository#children methods in the integration tests in this package.
 * It only implements AbstractBlobContainer#listBlobs, which is needed by RepositoryAnalyzeAction to verify child listings. All other
 * methods are stubbed out and throw UnsupportedOperationException.
 */
public class ChildBlobContainer extends AbstractBlobContainer {
    private final Map<String, byte[]> blobs;
    private final String prefix;

    ChildBlobContainer(BlobPath parentPath, String dirName, Map<String, byte[]> blobs) {
        super(parentPath.add(dirName));
        this.prefix = dirName + "/";
        this.blobs = blobs;
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return blobs.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(prefix.length()),
                    e -> new BlobMetadata(e.getKey().substring(prefix.length()), e.getValue().length)
                )
            );
    }

    @Override
    public boolean blobExists(OperationPurpose purpose, String blobName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlobAtomic(
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteResult delete(OperationPurpose purpose) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compareAndExchangeRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
        throw new UnsupportedOperationException();
    }
}
