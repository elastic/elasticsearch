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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.index.shard.ShardId;

import java.util.Collection;

public class IndexDirectory extends FilterDirectory {

    private final SearchDirectory searchDirectory;

    public IndexDirectory(Directory in, SharedBlobCacheService<FileCacheKey> cacheService, ShardId shardId) {
        super(in);
        this.searchDirectory = new SearchDirectory(cacheService, shardId);
    }

    @Override
    public void sync(Collection<String> names) {
        // noop, data on local drive need not be safely persisted
    }

    @Override
    public void syncMetaData() {
        // noop, data on local drive need not be safely persisted
    }

    public SearchDirectory getSearchDirectory() {
        return searchDirectory;
    }
}
