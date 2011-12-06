package org.elasticsearch.common.blobstore;

/**
 *
 */
public interface BlobStore {

    ImmutableBlobContainer immutableBlobContainer(BlobPath path);

    void delete(BlobPath path);

    void close();
}
