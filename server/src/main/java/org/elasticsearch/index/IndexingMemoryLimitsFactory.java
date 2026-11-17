/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;

/**
 * SPI for replacing the source of the {@link IndexingMemoryLimits}. At most one implementation may be provided. When there is none,
 * {@link SettingsIndexingMemoryLimits} is used.
 */
public interface IndexingMemoryLimitsFactory {

    /**
     * Create the limits for a node with the given settings. The limits are created before plugin components, so they can only depend on
     * node settings.
     */
    IndexingMemoryLimits create(Settings settings);
}
