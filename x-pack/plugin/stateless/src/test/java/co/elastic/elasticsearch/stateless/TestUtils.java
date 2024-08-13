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

import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.ThreadPool;

public class TestUtils {

    private TestUtils() {}

    public static StatelessSharedBlobCacheService newCacheService(
        NodeEnvironment nodeEnvironment,
        Settings settings,
        ThreadPool threadPool
    ) {
        return new StatelessSharedBlobCacheService(
            nodeEnvironment,
            settings,
            threadPool,
            Stateless.SHARD_READ_THREAD_POOL,
            BlobCacheMetrics.NOOP
        );
    }
}
