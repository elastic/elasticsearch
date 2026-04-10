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

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;

/**
 * Factory for creating {@link WarmingRatioProvider} instances.
 * The exact implementation is provided via SPI. If none is provided, {@link NoWarmingRatioProviderFactory} will be used.
 */
public interface WarmingRatioProviderFactory {
    WarmingRatioProvider create(ClusterSettings clusterSettings);
}
