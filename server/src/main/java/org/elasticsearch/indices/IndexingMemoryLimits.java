/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexingPressure;

/**
 * Memory limits applied to indexing-related workloads on a node.
 * The default implementation derives limits from the existing {@link IndexingPressure} and
 * {@link IndexingMemoryController} settings. Stateless deployments replace this with a
 * partition-based implementation so that back-pressure and buffer limits are driven by the
 * configured partition fractions rather than independent settings.
 *
 * <p>Limits are computed once at node startup and are fixed for the node's lifetime.
 */
public interface IndexingMemoryLimits {

    /** Max bytes for in-flight coordinating indexing operations before back-pressure is applied. */
    long coordinatingLimitBytes();

    /** Max bytes for in-flight primary indexing operations before back-pressure is applied. */
    long primaryLimitBytes();

    /** Max bytes for in-flight replica indexing operations before back-pressure is applied. */
    long replicaLimitBytes();

    /** Max bytes for a single indexing operation; larger operations are rejected. */
    long operationLimitBytes();

    /** Bytes allocated for Lucene indexing buffers; shards flush when this is approached. */
    long indexBufferBytes();

    /**
     * Returns an {@link IndexingMemoryLimits} derived from the standard
     * {@link IndexingPressure} and {@link IndexingMemoryController} settings.
     */
    static IndexingMemoryLimits fromSettings(Settings settings) {
        return new SettingsBasedIndexingMemoryLimits(settings);
    }
}
