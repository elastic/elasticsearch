/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.cluster.service;

/**
 * Listener interface for master task throttling
 */
public interface ClusterManagerTaskThrottlerListener {
    @SuppressWarnings("checkstyle:RedundantModifier")
    void onThrottle(String type, final int counts);
}
