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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * IndexStoreConfig encapsulates node / cluster level configuration for index level {@link IndexStore} instances.
 * For instance it maintains the node level rate limiter configuration: updates to the cluster that disable or enable
 * <tt>indices.store.throttle.type</tt> or <tt>indices.store.throttle.max_bytes_per_sec</tt> are reflected immediately
 * on all referencing {@link IndexStore} instances
 */
public class IndexStoreConfig {

    /**
     * Configures the node / cluster level throttle type. See {@link StoreRateLimiting.Type}.
     */
    public static final Setting<StoreRateLimiting.Type> INDICES_STORE_THROTTLE_TYPE_SETTING =
        new Setting<>("indices.store.throttle.type", StoreRateLimiting.Type.NONE.name(),StoreRateLimiting.Type::fromString,
            Property.Dynamic, Property.NodeScope);
    /**
     * Configures the node / cluster level throttle intensity. The default is <tt>10240 MB</tt>
     */
    public static final Setting<ByteSizeValue> INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING =
        Setting.byteSizeSetting("indices.store.throttle.max_bytes_per_sec", new ByteSizeValue(0),
            Property.Dynamic, Property.NodeScope);
    private volatile StoreRateLimiting.Type rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();
    private final ESLogger logger;
    public IndexStoreConfig(Settings settings) {
        logger = Loggers.getLogger(IndexStoreConfig.class, settings);
        // we don't limit by default (we default to CMS's auto throttle instead):
        this.rateLimitingType = INDICES_STORE_THROTTLE_TYPE_SETTING.get(settings);
        rateLimiting.setType(rateLimitingType);
        this.rateLimitingThrottle = INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC_SETTING.get(settings);
        rateLimiting.setMaxRate(rateLimitingThrottle);
        logger.debug("using indices.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);
    }

    /**
     * Returns the node level rate limiter
     */
    public StoreRateLimiting getNodeRateLimiter(){
        return rateLimiting;
    }

    public void setRateLimitingType(StoreRateLimiting.Type rateLimitingType) {
        this.rateLimitingType = rateLimitingType;
        rateLimiting.setType(rateLimitingType);
    }

    public void setRateLimitingThrottle(ByteSizeValue rateLimitingThrottle) {
        this.rateLimitingThrottle = rateLimitingThrottle;
    }
}
