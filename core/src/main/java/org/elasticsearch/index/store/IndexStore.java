/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
/**
 *
 */
public class IndexStore extends AbstractIndexComponent {
    public static final Setting<StoreRateLimiting.Type> INDEX_STORE_THROTTLE_TYPE_SETTING = new Setting<>("index.store.throttle.type", "none", StoreRateLimiting.Type::fromString, true, Setting.Scope.INDEX) ;
    public static final Setting<ByteSizeValue> INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING = Setting.byteSizeSetting("index.store.throttle.max_bytes_per_sec", new ByteSizeValue(0), true, Setting.Scope.INDEX);

    protected final IndexStoreConfig indexStoreConfig;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    public IndexStore(IndexSettings indexSettings, IndexStoreConfig indexStoreConfig) {
        super(indexSettings);
        this.indexStoreConfig = indexStoreConfig;
        rateLimiting.setType(indexSettings.getValue(INDEX_STORE_THROTTLE_TYPE_SETTING));
        rateLimiting.setMaxRate(indexSettings.getValue(INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING));
        logger.debug("using index.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimiting.getType(), rateLimiting.getRateLimiter());
    }

    /**
     * Returns the rate limiting, either of the index is explicitly configured, or
     * the node level one (defaults to the node level one).
     */
    public StoreRateLimiting rateLimiting() {
        return rateLimiting.getType() != StoreRateLimiting.Type.NONE ? indexStoreConfig.getNodeRateLimiter() : this.rateLimiting;
    }

    /**
     * The shard store class that should be used for each shard.
     */
    public DirectoryService newDirectoryService(ShardPath path) {
        return new FsDirectoryService(indexSettings, this, path);
    }

    public void setType(StoreRateLimiting.Type type) {
        rateLimiting.setType(type);
    }

    public void setMaxRate(ByteSizeValue rate) {
        rateLimiting.setMaxRate(rate);
    }
}
