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
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardPath;
/**
 *
 */
public class IndexStore extends AbstractIndexComponent {
    public static final Setting<IndexRateLimitingType> INDEX_STORE_THROTTLE_TYPE_SETTING =
        new Setting<>("index.store.throttle.type", "none", IndexRateLimitingType::fromString,
            Property.Dynamic, Property.IndexScope);
    public static final Setting<ByteSizeValue> INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING =
        Setting.byteSizeSetting("index.store.throttle.max_bytes_per_sec", new ByteSizeValue(0),
            Property.Dynamic, Property.IndexScope);

    protected final IndexStoreConfig indexStoreConfig;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();
    private volatile IndexRateLimitingType type;

    public IndexStore(IndexSettings indexSettings, IndexStoreConfig indexStoreConfig) {
        super(indexSettings);
        this.indexStoreConfig = indexStoreConfig;
        setType(indexSettings.getValue(INDEX_STORE_THROTTLE_TYPE_SETTING));
        rateLimiting.setMaxRate(indexSettings.getValue(INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING));
        logger.debug("using index.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimiting.getType(), rateLimiting.getRateLimiter());
    }

    /**
     * Returns the rate limiting, either of the index is explicitly configured, or
     * the node level one (defaults to the node level one).
     */
    public StoreRateLimiting rateLimiting() {
        return type.useStoreLimiter() ? indexStoreConfig.getNodeRateLimiter() : this.rateLimiting;
    }

    /**
     * The shard store class that should be used for each shard.
     */
    public DirectoryService newDirectoryService(ShardPath path) {
        return new FsDirectoryService(indexSettings, this, path);
    }

    public void setType(IndexRateLimitingType type) {
        this.type = type;
        if (type.useStoreLimiter() == false) {
            rateLimiting.setType(type.type);
        }
    }

    public void setMaxRate(ByteSizeValue rate) {
        rateLimiting.setMaxRate(rate);
    }

    /**
     * On an index level we can configure all of {@link org.apache.lucene.store.StoreRateLimiting.Type} as well as
     * <tt>node</tt> which will then use a global rate limiter that has it's own configuration. The global one is
     * configured in {@link IndexStoreConfig} which is managed by the per-node {@link org.elasticsearch.indices.IndicesService}
     */
    public static final class IndexRateLimitingType {
        private final StoreRateLimiting.Type type;

        private IndexRateLimitingType(StoreRateLimiting.Type type) {
            this.type = type;
        }

        private boolean useStoreLimiter() {
            return type == null;
        }

        static IndexRateLimitingType fromString(String type) {
            if ("node".equalsIgnoreCase(type)) {
                return new IndexRateLimitingType(null);
            } else {
                try {
                    return new IndexRateLimitingType(StoreRateLimiting.Type.fromString(type));
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException("rate limiting type [" + type + "] not valid, can be one of [all|merge|none|node]");
                }
            }
        }
    }

}
