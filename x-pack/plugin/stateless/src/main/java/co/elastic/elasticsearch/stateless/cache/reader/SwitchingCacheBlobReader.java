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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.core.Strings;

import java.io.InputStream;

/**
 * Switches between two {@link CacheBlobReader} implementations based on whether the batched compound commit has been uploaded to the
 * object store. The info of whether the commit has been uploaded is provided by a {@link ObjectStoreUploadTracker}.
 */
public class SwitchingCacheBlobReader implements CacheBlobReader {

    private static final Logger logger = LogManager.getLogger(SwitchingCacheBlobReader.class);

    private final UploadInfo latestUploadInfo;
    private final CacheBlobReader cacheBlobReaderForUploaded;
    private final CacheBlobReader cacheBlobReaderForNonUploaded;

    public SwitchingCacheBlobReader(
        UploadInfo latestUploadInfo,
        CacheBlobReader cacheBlobReaderForUploaded,
        CacheBlobReader cacheBlobReaderForNonUploaded
    ) {
        this.latestUploadInfo = latestUploadInfo;
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
                // TODO ideally use ShardReadThread pool here again. (ES-8155)
                // TODO ideally use a region-aligned range to write. (ES-8225)
                // TODO remove ResourceAlreadyUploadedException from core ES
                if (ExceptionsHelper.unwrapCause(ex) instanceof ResourceNotFoundException) {
                    logger.debug(
                        () -> Strings.format(
                            "switching blob reading from [%s] to [%s] due to exception",
                            cacheBlobReaderForNonUploaded,
                            cacheBlobReaderForUploaded
                        ),
                        ex
                    );
                    cacheBlobReaderForUploaded.getRangeInputStream(position, length, l);
                } else {
                    l.onFailure(ex);
                }
            }));
        }
    }
}
