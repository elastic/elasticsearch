/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.store.support;

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.store.IndicesStore;

import java.io.IOException;

/**
 *
 */
public abstract class AbstractIndexStore extends AbstractIndexComponent implements IndexStore {

    static {
        IndexMetaData.addDynamicSettings(
                "index.store.throttle.type",
                "index.store.throttle.max_bytes_per_sec"
        );
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String rateLimitingType = indexSettings.get("index.store.throttle.type", AbstractIndexStore.this.rateLimitingType);
            if (!rateLimitingType.equals(AbstractIndexStore.this.rateLimitingType)) {
                logger.info("updating index.store.throttle.type from [{}] to [{}]", AbstractIndexStore.this.rateLimitingType, rateLimitingType);
                if (rateLimitingType.equalsIgnoreCase("node")) {
                    AbstractIndexStore.this.rateLimitingType = rateLimitingType;
                    AbstractIndexStore.this.nodeRateLimiting = true;
                } else {
                    StoreRateLimiting.Type.fromString(rateLimitingType);
                    AbstractIndexStore.this.rateLimitingType = rateLimitingType;
                    AbstractIndexStore.this.nodeRateLimiting = false;
                    AbstractIndexStore.this.rateLimiting.setType(rateLimitingType);
                }
            }

            ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize("index.store.throttle.max_bytes_per_sec", AbstractIndexStore.this.rateLimitingThrottle);
            if (!rateLimitingThrottle.equals(AbstractIndexStore.this.rateLimitingThrottle)) {
                logger.info("updating index.store.throttle.max_bytes_per_sec from [{}] to [{}], note, type is [{}]", AbstractIndexStore.this.rateLimitingThrottle, rateLimitingThrottle, AbstractIndexStore.this.rateLimitingType);
                AbstractIndexStore.this.rateLimitingThrottle = rateLimitingThrottle;
                AbstractIndexStore.this.rateLimiting.setMaxRate(rateLimitingThrottle);
            }
        }
    }


    protected final IndexService indexService;

    protected final IndicesStore indicesStore;

    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private volatile boolean nodeRateLimiting;

    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final ApplySettings applySettings = new ApplySettings();

    protected AbstractIndexStore(Index index, @IndexSettings Settings indexSettings, IndexService indexService, IndicesStore indicesStore) {
        super(index, indexSettings);
        this.indexService = indexService;
        this.indicesStore = indicesStore;

        this.rateLimitingType = indexSettings.get("index.store.throttle.type", "node");
        if (rateLimitingType.equalsIgnoreCase("node")) {
            nodeRateLimiting = true;
        } else {
            nodeRateLimiting = false;
            rateLimiting.setType(rateLimitingType);
        }
        this.rateLimitingThrottle = indexSettings.getAsBytesSize("index.store.throttle.max_bytes_per_sec", new ByteSizeValue(0));
        rateLimiting.setMaxRate(rateLimitingThrottle);

        logger.debug("using index.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);

        indexService.settingsService().addListener(applySettings);
    }

    @Override
    public void close(boolean delete) throws ElasticSearchException {
        indexService.settingsService().removeListener(applySettings);
    }

    @Override
    public boolean canDeleteUnallocated(ShardId shardId) {
        return false;
    }

    @Override
    public void deleteUnallocated(ShardId shardId) throws IOException {
        // do nothing here...
    }

    @Override
    public IndicesStore indicesStore() {
        return indicesStore;
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return nodeRateLimiting ? indicesStore.rateLimiting() : this.rateLimiting;
    }
}
