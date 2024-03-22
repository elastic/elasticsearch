/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

/**
 * This is a strategy class for {@link StatelessMockRepository}.
 * <p>
 *
 * This class can be extended to add custom behavior to a repository for testing purposes. New methods should be added to this interface,
 * and called in a corresponding {@link StatelessMockRepository} method, as needed for new testing.
 * <p>
 *
 * Most of the methods receive the corresponding BlobStore/BlobContainer method as a lambda parameter (Supplier/Consumer) so that strategy
 * implementations can control before and after actions, as well as whether to call the real logic at all or instead return an artificially
 * constructed result.
 */
public class StatelessMockRepositoryStrategy {
    /**
     * Called in {@link BlobStore#deleteBlobsIgnoringIfNotExists(OperationPurpose, Iterator)}.
     */
    public void blobStoreDeleteBlobsIgnoringIfNotExists(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        Iterator<String> blobNames
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#children(OperationPurpose)}.
     */
    public Map<String, BlobContainer> blobContainerChildren(
        CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
        OperationPurpose purpose
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#delete(OperationPurpose)}.
     */
    public DeleteResult blobContainerDelete(CheckedSupplier<DeleteResult, IOException> originalSupplier, OperationPurpose purpose)
        throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#readBlob(OperationPurpose, String)}.
     */
    public InputStream blobContainerReadBlob(
        CheckedSupplier<InputStream, IOException> originalSupplier,
        OperationPurpose purpose,
        String blobName
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#readBlob(OperationPurpose, String, long, long)}.
     */
    public InputStream blobContainerReadBlob(
        CheckedSupplier<InputStream, IOException> originalSupplier,
        OperationPurpose purpose,
        String blobName,
        long position,
        long length
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#writeBlob(OperationPurpose, String, InputStream, long, boolean)}.
     */
    public void blobContainerWriteBlob(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#writeBlob(OperationPurpose, String, BytesReference, boolean)}.
     */
    public void blobContainerWriteBlob(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        String blobName,
        BytesReference bytes,
        boolean failIfAlreadyExists
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#writeMetadataBlob(OperationPurpose, String, boolean, boolean, CheckedConsumer)}.
     */
    public void blobContainerWriteMetadataBlob(
        CheckedRunnable<IOException> original,
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        original.run();
    }

    /**
     * Called in {@link BlobContainer#listBlobs(OperationPurpose)}.
     */
    public Map<String, BlobMetadata> blobContainerListBlobs(
        CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
        OperationPurpose purpose
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#listBlobsByPrefix(OperationPurpose, String)}.
     */
    public Map<String, BlobMetadata> blobContainerListBlobsByPrefix(
        CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
        OperationPurpose purpose,
        String blobNamePrefix
    ) throws IOException {
        return originalSupplier.get();
    }

}
