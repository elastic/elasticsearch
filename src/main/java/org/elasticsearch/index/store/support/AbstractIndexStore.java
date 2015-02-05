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

package org.elasticsearch.index.store.support;

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.indices.store.IndicesStore;

import java.io.IOException;
import java.nio.file.Path;

/**
 *
 */
public abstract class AbstractIndexStore extends AbstractIndexComponent implements IndexStore {

    public static final String INDEX_STORE_THROTTLE_TYPE = "index.store.throttle.type";
    public static final String INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC = "index.store.throttle.max_bytes_per_sec";

    public static final String INDEX_FOLDER_NAME = "index";
    public static final String TRANSLOG_FOLDER_NAME = "translog";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String rateLimitingType = settings.get(INDEX_STORE_THROTTLE_TYPE, AbstractIndexStore.this.rateLimitingType);
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

            ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize(INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, AbstractIndexStore.this.rateLimitingThrottle);
            if (!rateLimitingThrottle.equals(AbstractIndexStore.this.rateLimitingThrottle)) {
                logger.info("updating index.store.throttle.max_bytes_per_sec from [{}] to [{}], note, type is [{}]", AbstractIndexStore.this.rateLimitingThrottle, rateLimitingThrottle, AbstractIndexStore.this.rateLimitingType);
                AbstractIndexStore.this.rateLimitingThrottle = rateLimitingThrottle;
                AbstractIndexStore.this.rateLimiting.setMaxRate(rateLimitingThrottle);
            }
        }
    }
    private final NodeEnvironment nodeEnv;

    private final Path[] locations;

    protected final IndexService indexService;

    protected final IndicesStore indicesStore;

    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private volatile boolean nodeRateLimiting;

    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final ApplySettings applySettings = new ApplySettings();

    protected AbstractIndexStore(Index index, @IndexSettings Settings indexSettings, IndexService indexService, IndicesStore indicesStore, NodeEnvironment nodeEnv) {
        super(index, indexSettings);
        this.indexService = indexService;
        this.indicesStore = indicesStore;

        this.rateLimitingType = indexSettings.get(INDEX_STORE_THROTTLE_TYPE, "node");
        if (rateLimitingType.equalsIgnoreCase("node")) {
            nodeRateLimiting = true;
        } else {
            nodeRateLimiting = false;
            rateLimiting.setType(rateLimitingType);
        }
        this.rateLimitingThrottle = indexSettings.getAsBytesSize(INDEX_STORE_THROTTLE_MAX_BYTES_PER_SEC, new ByteSizeValue(0));
        rateLimiting.setMaxRate(rateLimitingThrottle);

        logger.debug("using index.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);

        indexService.settingsService().addListener(applySettings);
        this.nodeEnv = nodeEnv;
        if (nodeEnv.hasNodeFile()) {
            this.locations = nodeEnv.indexPaths(index);
        } else {
            this.locations = null;
        }
    }

    @Override
    public IndicesStore indicesStore() {
        return this.indicesStore;
    }

    @Override
    public void close() throws ElasticsearchException {
        indexService.settingsService().removeListener(applySettings);
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return nodeRateLimiting ? indicesStore.rateLimiting() : this.rateLimiting;
    }

    /**
     * Return an array of all index folder locations for a given shard. Uses
     * the index settings to determine if a custom data path is set for the
     * index and uses that if applicable.
     */
    public Path[] shardIndexLocations(ShardId shardId) {
        Path[] shardLocations = nodeEnv.shardDataPaths(shardId, indexSettings);
        Path[] locations = new Path[shardLocations.length];
        for (int i = 0; i < shardLocations.length; i++) {
            locations[i] = shardLocations[i].resolve(INDEX_FOLDER_NAME);
        }
        logger.debug("using [{}] as shard's index location", locations);
        return locations;
    }

    /**
     * Return an array of all translog folder locations for a given shard. Uses
     * the index settings to determine if a custom data path is set for the
     * index and uses that if applicable.
     */
    public Path[] shardTranslogLocations(ShardId shardId) {
        Path[] shardLocations = nodeEnv.shardDataPaths(shardId, indexSettings);
        Path[] locations = new Path[shardLocations.length];
        for (int i = 0; i < shardLocations.length; i++) {
            locations[i] = shardLocations[i].resolve(TRANSLOG_FOLDER_NAME);
        }
        logger.debug("using [{}] as shard's translog location", locations);
        return locations;
    }
}
