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

import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

/**
 * SPI for computing the warming ratio for a compound commit.
 * Implementation is to be provided via {@link WarmingRatioProviderFactory}.
 */
public interface WarmingRatioProvider {
    /**
     * Computes the warming ratio for a compound commit, determining what fraction of its data
     * should be pre-warmed into the shared blob cache. Currently, only used on search nodes.
     * A ratio of 0 means no pre-warming; 1 means full pre-warming.
     */
    double getWarmingRatio(ObjectStoreService.StatelessCompoundCommitReferenceWithInternalFiles referencedCompoundCommit, long nowMillis);
}
