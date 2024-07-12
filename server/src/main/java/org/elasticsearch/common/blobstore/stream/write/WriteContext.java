/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.common.blobstore.stream.write;


import org.elasticsearch.common.StreamContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * WriteContext is used to encapsulate all data needed by <code>BlobContainer#writeStreams</code>
 *
 * @opensearch.internal
 */
public class WriteContext {

    private final String fileName;
    private final StreamContextSupplier streamContextSupplier;
    private final long fileSize;
    private final boolean failIfAlreadyExists;
    private final WritePriority writePriority;
    private final CheckedConsumer<Boolean, IOException> uploadFinalizer;
    private final boolean doRemoteDataIntegrityCheck;
    private final Long expectedChecksum;
    private final Map<String, String> metadata;

    /**
     * Construct a new WriteContext object
     *
     * @param fileName                   The name of the file being uploaded
     * @param streamContextSupplier      A supplier that will provide StreamContext to the plugin
     * @param fileSize                   The total size of the file being uploaded
     * @param failIfAlreadyExists        A boolean to fail the upload is the file exists
     * @param writePriority              The <code>WritePriority</code> of this upload
     * @param doRemoteDataIntegrityCheck A boolean to inform vendor plugins whether remote data integrity checks need to be done
     * @param expectedChecksum           This parameter expected only when the vendor plugin is expected to do server side data integrity verification
     */
    @SuppressWarnings("checkstyle:LineLength")
    private WriteContext(
        String fileName,
        StreamContextSupplier streamContextSupplier,
        long fileSize,
        boolean failIfAlreadyExists,
        WritePriority writePriority,
        CheckedConsumer<Boolean, IOException> uploadFinalizer,
        boolean doRemoteDataIntegrityCheck,
        @Nullable Long expectedChecksum,
        @Nullable Map<String, String> metadata
    ) {
        this.fileName = fileName;
        this.streamContextSupplier = streamContextSupplier;
        this.fileSize = fileSize;
        this.failIfAlreadyExists = failIfAlreadyExists;
        this.writePriority = writePriority;
        this.uploadFinalizer = uploadFinalizer;
        this.doRemoteDataIntegrityCheck = doRemoteDataIntegrityCheck;
        this.expectedChecksum = expectedChecksum;
        this.metadata = metadata;
    }

    /**
     * Copy constructor used by overriding class
     */
    protected WriteContext(WriteContext writeContext) {
        this.fileName = writeContext.fileName;
        this.streamContextSupplier = writeContext.streamContextSupplier;
        this.fileSize = writeContext.fileSize;
        this.failIfAlreadyExists = writeContext.failIfAlreadyExists;
        this.writePriority = writeContext.writePriority;
        this.uploadFinalizer = writeContext.uploadFinalizer;
        this.doRemoteDataIntegrityCheck = writeContext.doRemoteDataIntegrityCheck;
        this.expectedChecksum = writeContext.expectedChecksum;
        this.metadata = writeContext.metadata;
    }

    /**
     * @return The file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * @return The boolean representing whether to fail the file upload if it exists
     */
    public boolean isFailIfAlreadyExists() {
        return failIfAlreadyExists;
    }

    /**
     * @param partSize The size of a single part to be uploaded
     * @return The stream context which will be used by the plugin to initialize streams from the file
     */
    public StreamContext getStreamProvider(long partSize) {
        return streamContextSupplier.supplyStreamContext(partSize);
    }

    /**
     * @return The total size of the file
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * @return The <code>WritePriority</code> of the upload
     */
    public WritePriority getWritePriority() {
        return writePriority;
    }

    /**
     * @return The <code>UploadFinalizer</code> for this upload
     */
    public CheckedConsumer<Boolean, IOException> getUploadFinalizer() {
        return uploadFinalizer;
    }

    /**
     * @return A boolean for whether remote data integrity check has to be done for this upload or not
     */
    public boolean doRemoteDataIntegrityCheck() {
        return doRemoteDataIntegrityCheck;
    }

    /**
     * @return The CRC32 checksum associated with this file
     */
    public Long getExpectedChecksum() {
        return expectedChecksum;
    }

    /**
     * @return the upload metadata.
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Builder for {@link WriteContext}.
     *
     * @opensearch.internal
     */
    public static class Builder {
        private String fileName;
        private StreamContextSupplier streamContextSupplier;
        private long fileSize;
        private boolean failIfAlreadyExists;
        private WritePriority writePriority;
        private CheckedConsumer<Boolean, IOException> uploadFinalizer;
        private boolean doRemoteDataIntegrityCheck;
        private Long expectedChecksum;
        private Map<String, String> metadata;

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder streamContextSupplier(StreamContextSupplier streamContextSupplier) {
            this.streamContextSupplier = streamContextSupplier;
            return this;
        }

        public Builder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public Builder writePriority(WritePriority writePriority) {
            this.writePriority = writePriority;
            return this;
        }

        public Builder failIfAlreadyExists(boolean failIfAlreadyExists) {
            this.failIfAlreadyExists = failIfAlreadyExists;
            return this;
        }

        public Builder uploadFinalizer(CheckedConsumer<Boolean, IOException> uploadFinalizer) {
            this.uploadFinalizer = uploadFinalizer;
            return this;
        }

        public Builder doRemoteDataIntegrityCheck(boolean doRemoteDataIntegrityCheck) {
            this.doRemoteDataIntegrityCheck = doRemoteDataIntegrityCheck;
            return this;
        }

        public Builder expectedChecksum(Long expectedChecksum) {
            this.expectedChecksum = expectedChecksum;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public WriteContext build() {
            return new WriteContext(
                fileName,
                streamContextSupplier,
                fileSize,
                failIfAlreadyExists,
                writePriority,
                uploadFinalizer,
                doRemoteDataIntegrityCheck,
                expectedChecksum,
                metadata
            );
        }
    }
}
