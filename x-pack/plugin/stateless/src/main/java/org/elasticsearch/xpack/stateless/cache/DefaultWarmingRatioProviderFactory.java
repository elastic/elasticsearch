/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein are
 * proprietary to Elasticsearch B.V. and its suppliers and may be
 * covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

/**
 * Default {@link WarmingRatioProviderFactory} when no SPI implementation is registered: uses
 * {@link DefaultWarmingRatioProviderFactory#SEARCH_RECOVERY_WARMING_RATIO_SETTING} (default 0 = no BCC warming by ratio).
 *
 * This is a temporary means until we define the warming ratio or search power properly for stateless
 */
public class DefaultWarmingRatioProviderFactory implements WarmingRatioProviderFactory {

    /**
     * When no {@link WarmingRatioProviderFactory} SPI is registered, fraction of each compound commit to warm during search recovery
     * BCC warming (0 = off, 1 = full). Default 0. Ignored when an SPI implementation is present.
     */
    public static final Setting<Double> SEARCH_RECOVERY_WARMING_RATIO_SETTING = Setting.doubleSetting(
        SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_SETTING_PREFIX_NAME + ".recovery_warming_ratio",
        0.0d,
        0.0d,
        1.0d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Override
    public WarmingRatioProvider create(ClusterSettings clusterSettings) {
        return new DefaultWarmingRatioProvider(clusterSettings);
    }

    static final class DefaultWarmingRatioProvider implements WarmingRatioProvider {

        private volatile double ratio;

        DefaultWarmingRatioProvider(ClusterSettings clusterSettings) {
            if (clusterSettings.get(SEARCH_RECOVERY_WARMING_RATIO_SETTING.getKey()) != null) {
                clusterSettings.initializeAndWatch(SEARCH_RECOVERY_WARMING_RATIO_SETTING, v -> this.ratio = v);
            }
        }

        @Override
        public double getWarmingRatio(
            ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCompoundCommit,
            long nowMillis
        ) {
            return ratio;
        }
    }
}
