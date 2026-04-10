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
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

/**
 * A {@link WarmingRatioProviderFactory} that creates a provider which disables pre-warming
 * by always returning a warming ratio of 0.
 */
public class NoWarmingRatioProviderFactory implements WarmingRatioProviderFactory {

    @Override
    public WarmingRatioProvider create(ClusterSettings clusterSettings) {
        return new NoWarmingRatioProvider();
    }

    static class NoWarmingRatioProvider implements WarmingRatioProvider {

        @Override
        public double getWarmingRatio(
            ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCompoundCommit,
            long nowMillis
        ) {
            return 0;
        }
    }
}
