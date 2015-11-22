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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * IndexStoreConfig encapsulates node / cluster level configuration for index level {@link IndexStore} instances.
 * For instance it maintains the node level rate limiter configuration: updates to the cluster that disable or enable
 * {@value #INDICES_STORE_THROTTLE_TYPE} or {@value #INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC} are reflected immediately
 * on all referencing {@link IndexStore} instances
 */
public class IndexStoreConfig implements NodeSettingsService.Listener {

    /**
     * Configures the node / cluster level throttle type. See {@link StoreRateLimiting.Type}.
     */
    public static final String INDICES_STORE_THROTTLE_TYPE = "indices.store.throttle.type";
    /**
     * Configures the node / cluster level throttle intensity. The default is <tt>10240 MB</tt>
     */
    public static final String INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC = "indices.store.throttle.max_bytes_per_sec";
    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();
    private final ESLogger logger;
    public IndexStoreConfig(Settings settings) {
        logger = Loggers.getLogger(IndexStoreConfig.class, settings);
        // we don't limit by default (we default to CMS's auto throttle instead):
        this.rateLimitingType = settings.get("indices.store.throttle.type", StoreRateLimiting.Type.NONE.name());
        rateLimiting.setType(rateLimitingType);
        this.rateLimitingThrottle = settings.getAsBytesSize("indices.store.throttle.max_bytes_per_sec", new ByteSizeValue(0));
        rateLimiting.setMaxRate(rateLimitingThrottle);
        logger.debug("using indices.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);
    }

    /**
     * Returns the node level rate limiter
     */
    public StoreRateLimiting getNodeRateLimiter(){
        return rateLimiting;
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        String rateLimitingType = settings.get(INDICES_STORE_THROTTLE_TYPE, this.rateLimitingType);
        // try and parse the type
        StoreRateLimiting.Type.fromString(rateLimitingType);
        if (!rateLimitingType.equals(this.rateLimitingType)) {
            logger.info("updating indices.store.throttle.type from [{}] to [{}]", this.rateLimitingType, rateLimitingType);
            this.rateLimitingType = rateLimitingType;
            this.rateLimiting.setType(rateLimitingType);
        }

        ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize(INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC, this.rateLimitingThrottle);
        if (!rateLimitingThrottle.equals(this.rateLimitingThrottle)) {
            logger.info("updating indices.store.throttle.max_bytes_per_sec from [{}] to [{}], note, type is [{}]", this.rateLimitingThrottle, rateLimitingThrottle, this.rateLimitingType);
            this.rateLimitingThrottle = rateLimitingThrottle;
            this.rateLimiting.setMaxRate(rateLimitingThrottle);
        }
    }
}
