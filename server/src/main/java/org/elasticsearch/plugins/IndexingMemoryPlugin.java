/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexingMemoryLimits;

/**
 * Plugin interface for overriding the memory limits applied to indexing-related workloads.
 * At most one installed plugin may implement this interface.
 *
 * <p>Implement this to replace the default settings-based limits ({@link IndexingMemoryLimits#fromSettings})
 * with partition-derived limits, as stateless deployments do via their partitioned memory model.
 */
public interface IndexingMemoryPlugin {

    /**
     * Returns the {@link IndexingMemoryLimits} to use for this node, given the node-level settings.
     * Called once during node construction; the returned limits are used for the lifetime of the node.
     */
    IndexingMemoryLimits getIndexingMemoryLimits(Settings settings);
}
