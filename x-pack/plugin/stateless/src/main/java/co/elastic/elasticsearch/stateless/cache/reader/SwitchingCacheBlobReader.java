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

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.blobcache.common.ByteRange;

import java.io.IOException;
import java.io.InputStream;

/**
 * Switches between two {@link CacheBlobReader} implementations based on whether the batched compound commit has been uploaded to the
 * object store. The info of whether the commit has been uploaded is provided by a {@link ObjectStoreUploadTracker}.
 */
public class SwitchingCacheBlobReader implements CacheBlobReader {

    private final PrimaryTermAndGeneration locationPrimaryTermAndGeneration;
    private final ObjectStoreUploadTracker objectStoreUploadTracker;
    private final CacheBlobReader cacheBlobReaderForUploaded;
    private final CacheBlobReader cacheBlobReaderForNonUploaded;

    public SwitchingCacheBlobReader(
        PrimaryTermAndGeneration locationPrimaryTermAndGeneration,
        ObjectStoreUploadTracker objectStoreUploadTracker,
        CacheBlobReader cacheBlobReaderForUploaded,
        CacheBlobReader cacheBlobReaderForNonUploaded
    ) {
        this.locationPrimaryTermAndGeneration = locationPrimaryTermAndGeneration;
        this.objectStoreUploadTracker = objectStoreUploadTracker;
        this.cacheBlobReaderForUploaded = cacheBlobReaderForUploaded;
        this.cacheBlobReaderForNonUploaded = cacheBlobReaderForNonUploaded;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        if (objectStoreUploadTracker.isUploaded(locationPrimaryTermAndGeneration)) {
            return cacheBlobReaderForUploaded.getRange(position, length, remainingFileLength);
        } else {
            return cacheBlobReaderForNonUploaded.getRange(position, length, remainingFileLength);
        }
    }

    @Override
    public InputStream getRangeInputStream(long position, int length) throws IOException {
        if (objectStoreUploadTracker.isUploaded(locationPrimaryTermAndGeneration)) {
            return cacheBlobReaderForUploaded.getRangeInputStream(position, length);
        } else {
            try {
                return cacheBlobReaderForNonUploaded.getRangeInputStream(position, length);
            } catch (Exception ex) {
                // TODO ideally use ShardReadThread pool here again. (ES-8155)
                // TODO ideally use a region-aligned range to write. (ES-8225)
                // TODO remove ResourceAlreadyUploadedException from core ES
                if (ExceptionsHelper.unwrapCause(ex) instanceof ResourceNotFoundException) {
                    return cacheBlobReaderForUploaded.getRangeInputStream(position, length);
                } else {
                    throw ex;
                }
            }
        }
    }
}
