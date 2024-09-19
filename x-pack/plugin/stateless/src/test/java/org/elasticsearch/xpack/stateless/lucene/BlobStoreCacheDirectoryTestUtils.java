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

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheFileReaderTestUtils;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

public class BlobStoreCacheDirectoryTestUtils {

    private BlobStoreCacheDirectoryTestUtils() {}

    public static BlobLocation getBlobLocation(BlobStoreCacheDirectory target, String fileName) {
        return target.getBlobLocation(fileName);
    }

    public static StatelessSharedBlobCacheService.CacheFile getCacheFile(BlobCacheIndexInput target) {
        return CacheFileReaderTestUtils.getCacheFile(target.getCacheFileReader());
    }

    public static StatelessSharedBlobCacheService getCacheService(BlobStoreCacheDirectory target) {
        return target.getCacheService();
    }

    public static void updateLatestUploadedBcc(SearchDirectory target, PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
        target.updateLatestUploadedBcc(latestUploadedBccTermAndGen);
    }

    public static void updateLatestCommitInfo(SearchDirectory target, PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
        target.updateLatestCommitInfo(ccTermAndGen, nodeId);
    }
}
