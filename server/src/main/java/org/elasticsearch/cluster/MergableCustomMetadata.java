/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.Metadata;

/**
 * Interface to allow merging {@link Metadata.Custom}.
 * When multiple Mergable Custom metadata of the same type are found (from underlying clusters), the
 * Custom metadata can be merged using {@link #merge(Metadata.Custom)}.
 *
 * @param <T> type of custom meta data
 */
public interface MergableCustomMetadata<T extends Metadata.Custom> {

    /**
     * Merges this custom metadata with other, returning either this or <code>other</code> custom metadata.
     * This method should not mutate either <code>this</code> or the <code>other</code> custom metadata.
     *
     * @param other custom meta data
     * @return the same instance or <code>other</code> custom metadata based on implementation
     *         if both the instances are considered equal, implementations should return this
     *         instance to avoid redundant cluster state changes.
     */
    T merge(T other);
}
