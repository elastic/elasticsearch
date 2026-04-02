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

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.utils.SearchShardSizeCollector;

/**
 * SPI for extending plugins that need to receive stateless services from the stateless plugin.
 */
public interface StatelessExtensionProvider {

    /**
     * Callback invoked for extending plugins to allow them to access the stateless services.
     */
    default void onServicesCreated(
        ClosedShardService closedShardService,
        HollowShardsService hollowShardsService,
        SearchShardSizeCollector searchShardSizeCollector,
        StatelessMemoryMetricsService statelessMemoryMetricsService,
        ObjectStoreService objectStoreService
    ) {}
}
