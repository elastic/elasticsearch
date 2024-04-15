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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.cache.reader;

import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;

import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link CacheBlobReader} that fetches region-aligned data from the object store.
 */
public class ObjectStoreCacheBlobReader implements CacheBlobReader {

    private final BlobContainer blobContainer;
    private final String blobName;
    private final long cacheRangeSize;

    public ObjectStoreCacheBlobReader(BlobContainer blobContainer, String blobName, long cacheRangeSize) {
        this.blobContainer = blobContainer;
        this.blobName = blobName;
        this.cacheRangeSize = cacheRangeSize;
    }

    @Override
    public ByteRange getRange(long position, int length) {
        return BlobCacheUtils.computeRange(cacheRangeSize, position, length);
    }

    @Override
    public InputStream getRangeInputStream(long position, int length) throws IOException {
        // TODO catch RequestedRangeNotSatisfiedException, ignore it
        return blobContainer.readBlob(OperationPurpose.INDICES, blobName, position, length);
    }
}
