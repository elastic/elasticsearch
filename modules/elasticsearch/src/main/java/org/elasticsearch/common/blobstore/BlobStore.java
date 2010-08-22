package org.elasticsearch.common.blobstore;

/**
 * @author kimchy (shay.banon)
 */
public interface BlobStore {

    ImmutableBlobContainer immutableBlobContainer(BlobPath path);

    void delete(BlobPath path);

    void close();
}
