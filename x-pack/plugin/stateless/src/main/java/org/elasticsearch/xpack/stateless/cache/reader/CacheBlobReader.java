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

import org.elasticsearch.blobcache.common.ByteRange;

import java.io.IOException;
import java.io.InputStream;

/**
 * Used by {@link co.elastic.elasticsearch.stateless.lucene.SearchIndexInput} to read data from the blob store or from the primary shard
 * in order to populate the cache for a specific blob.
 */
public interface CacheBlobReader {

    /**
     * Gets a range of a blob to read into the cache. The range can extend beyond the actual length of a blob, and it is expected
     * that no bytes beyond the actual length of the blob are fetched.
     * <p>
     * It is important that the end position of the resulting {@link ByteRange} is something that the {@link CacheBlobReader} (and any
     * {@link CacheBlobReader} switched to afterward) can actually fetch. In other words, the {@link IndexingShardCacheBlobReader} cannot
     * let the upper bound be more than position+length rounded up to next page.
     * <p>
     * Also, the {@link IndexingShardCacheBlobReader} cannot be used based on a range retrieved from the {@link ObjectStoreCacheBlobReader}.
     *
     * @param position            the position of the range to read into the cache
     * @param length              the length of the range to read into the cache
     * @param remainingFileLength the remaining length of the file, those bytes are guaranteed to be available.
     * @return the range to read into the cache
     */
    ByteRange getRange(long position, int length, long remainingFileLength);

    /**
     * The input stream to fetch the data from, with which to read into the cache. This may be called multiple times for different parts
     * of the range returned by {@link #getRange(long, int, long)}.
     *
     * It is OK for the {@link InputStream} to return less data than specified length (even no data).
     *
     * @param position the position of the blob to fetch data from
     * @param length the length to read from the blob starting from position
     * @return the input stream to fetch the data from
     */
    InputStream getRangeInputStream(long position, int length) throws IOException;

}
