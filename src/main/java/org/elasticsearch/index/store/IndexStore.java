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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.indices.store.IndicesStore;

import java.io.Closeable;

/**
 *
 */
public class IndexStore extends AbstractIndexComponent implements Closeable {

    public static final String INDEX_STORE_THROTTLE_TYPE = "index.store.throttle.type";
    public static final String INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC = "index.store.throttle.max_bytes_per_sec";

    private final IndexSettingsService settingsService;

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String rateLimitingType = settings.get(INDEX_STORE_THROTTLE_TYPE, IndexStore.this.rateLimitingType);
            if (!rateLimitingType.equals(IndexStore.this.rateLimitingType)) {
                logger.info("updating index.store.throttle.type from [{}] to [{}]", IndexStore.this.rateLimitingType, rateLimitingType);
                if (rateLimitingType.equalsIgnoreCase("node")) {
                    IndexStore.this.rateLimitingType = rateLimitingType;
                    IndexStore.this.nodeRateLimiting = true;
                } else {
                    StoreRateLimiting.Type.fromString(rateLimitingType);
                    IndexStore.this.rateLimitingType = rateLimitingType;
                    IndexStore.this.nodeRateLimiting = false;
                    IndexStore.this.rateLimiting.setType(rateLimitingType);
                }
            }

            ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize(INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, IndexStore.this.rateLimitingThrottle);
            if (!rateLimitingThrottle.equals(IndexStore.this.rateLimitingThrottle)) {
                logger.info("updating index.store.throttle.max_bytes_per_sec from [{}] to [{}], note, type is [{}]", IndexStore.this.rateLimitingThrottle, rateLimitingThrottle, IndexStore.this.rateLimitingType);
                IndexStore.this.rateLimitingThrottle = rateLimitingThrottle;
                IndexStore.this.rateLimiting.setMaxRate(rateLimitingThrottle);
            }
        }
    }
    protected final IndicesStore indicesStore;

    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private volatile boolean nodeRateLimiting;

    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public IndexStore(Index index, @IndexSettings Settings indexSettings, IndexSettingsService settingsService, IndicesStore indicesStore) {
        super(index, indexSettings);
         this.indicesStore = indicesStore;

        this.rateLimitingType = indexSettings.get(INDEX_STORE_THROTTLE_TYPE, "none");
        if (rateLimitingType.equalsIgnoreCase("node")) {
            nodeRateLimiting = true;
        } else {
            nodeRateLimiting = false;
            rateLimiting.setType(rateLimitingType);
        }
        this.rateLimitingThrottle = indexSettings.getAsBytesSize(INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, new ByteSizeValue(0));
        rateLimiting.setMaxRate(rateLimitingThrottle);

        logger.debug("using index.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);
        this.settingsService = settingsService;
        this.settingsService.addListener(applySettings);
    }

    @Override
    public void close() throws ElasticsearchException {
        settingsService.removeListener(applySettings);
    }

    /**
     * Returns the rate limiting, either of the index is explicitly configured, or
     * the node level one (defaults to the node level one).
     */
    public StoreRateLimiting rateLimiting() {
        return nodeRateLimiting ? indicesStore.rateLimiting() : this.rateLimiting;
    }

    /**
     * The shard store class that should be used for each shard.
     */
    public Class<? extends DirectoryService> shardDirectory() {
        return FsDirectoryService.class;
    }
}
