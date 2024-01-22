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

import co.elastic.elasticsearch.stateless.commits.BlobLocation;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;

import java.util.Map;

public class SearchDirectoryTestUtils {

    private SearchDirectoryTestUtils() {}

    public static void setMetadata(SearchDirectory target, Map<String, BlobLocation> source) {
        target.setMetadata(source);
    }

    public static BlobLocation getBlobLocation(SearchDirectory target, String fileName) {
        return target.getBlobLocation(fileName);
    }

    public static FileCacheKey getCacheKey(SearchIndexInput target) {
        return target.cacheFile().getCacheKey();
    }

    public static SharedBlobCacheService<FileCacheKey> getCacheService(SearchDirectory target) {
        return target.getCacheService();
    }
}
