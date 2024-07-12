/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.common.cache;

/**
 * A class that can determine the weight of a value. The total weight threshold
 * is used to determine when an eviction is required.
 *
 * @opensearch.internal
 */
public interface Weigher<V> {

    /**
     * Measures an object's weight to determine how many units of capacity that
     * the value consumes. A value must consume a minimum of one unit.
     *
     * @param value the object to weigh
     * @return the object's weight
     */
    long weightOf(V value);
}
