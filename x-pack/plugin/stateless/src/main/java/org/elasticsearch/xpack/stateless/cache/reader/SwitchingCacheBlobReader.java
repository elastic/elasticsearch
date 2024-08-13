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

import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreUploadTracker.UploadInfo;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.core.Strings;

import java.io.InputStream;

/**
 * Switches between two {@link CacheBlobReader} implementations based on whether the batched compound commit has been uploaded to the
 * object store. The info of whether the commit has been uploaded is provided by a {@link MutableObjectStoreUploadTracker}.
 * May throw {@link org.elasticsearch.ResourceAlreadyUploadedException} if the fetch from the indexing node resulted in a
 * {@link org.elasticsearch.ResourceNotFoundException} that the commit has been uploaded in the meantime, in which case the
 * {@link MutableObjectStoreUploadTracker} is updated, and the fetch needs to be re-tried (this time from the object store).
 */
public class SwitchingCacheBlobReader implements CacheBlobReader {

    private static final Logger logger = LogManager.getLogger(SwitchingCacheBlobReader.class);

    private final MutableObjectStoreUploadTracker tracker;
    private final PrimaryTermAndGeneration locationPrimaryTermAndGeneration;
    private final UploadInfo latestUploadInfo;
    private final CacheBlobReader cacheBlobReaderForUploaded;
    private final CacheBlobReader cacheBlobReaderForNonUploaded;

    public SwitchingCacheBlobReader(
        MutableObjectStoreUploadTracker tracker,
        PrimaryTermAndGeneration locationPrimaryTermAndGeneration,
        CacheBlobReader cacheBlobReaderForUploaded,
        CacheBlobReader cacheBlobReaderForNonUploaded
    ) {
        this.tracker = tracker;
        this.locationPrimaryTermAndGeneration = locationPrimaryTermAndGeneration;
        this.latestUploadInfo = tracker.getLatestUploadInfo(locationPrimaryTermAndGeneration);
        this.cacheBlobReaderForUploaded = cacheBlobReaderForUploaded;
        this.cacheBlobReaderForNonUploaded = cacheBlobReaderForNonUploaded;
    }

    @Override
    public ByteRange getRange(long position, int length, long remainingFileLength) {
        if (latestUploadInfo.isUploaded()) {
            return cacheBlobReaderForUploaded.getRange(position, length, remainingFileLength);
        } else {
            return cacheBlobReaderForNonUploaded.getRange(position, length, remainingFileLength);
        }
    }

    @Override
    public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
        if (latestUploadInfo.isUploaded()) {
            cacheBlobReaderForUploaded.getRangeInputStream(position, length, listener);
        } else {
            cacheBlobReaderForNonUploaded.getRangeInputStream(position, length, listener.delegateResponse((l, ex) -> {
                final var resourceNotFoundException = ExceptionsHelper.unwrap(ex, ResourceNotFoundException.class);
                if (resourceNotFoundException != null) {
                    final var message = Strings.format(
                        "[%s] replied that [%s] is not found",
                        cacheBlobReaderForNonUploaded,
                        locationPrimaryTermAndGeneration
                    );
                    logger.debug(() -> message, ex);

                    // Update the tracker so that next attempts will not try to read from the VBCC, but from the uploaded BCC.
                    tracker.updateLatestUploadedBcc(locationPrimaryTermAndGeneration);

                    // Throw an exception to indicate that the VBCC was uploaded and the read should be attempted again so that it
                    // reaches out to the BCC on the object store. Note that multiple concurrent reads may fail at this point, but they
                    // should be able to retry reading successfully from the object store.
                    // TODO think about a higher level abstraction that could hide/enforce the exception bubbling and/or the retry (ES-9052)
                    l.onFailure(new ResourceAlreadyUploadedException(message, ex));
                } else {
                    l.onFailure(ex);
                }
            }));
        }
    }
}
