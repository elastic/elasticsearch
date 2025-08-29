/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContent;

/**
 * An extension point for {@link Plugin} implementations that wish to contribute to node stats.
 */
public interface NodeStatsPlugin {

    /** Plugin name. Used for field key for stats. Must be unique across all plugins. */
    String getNodeStatsPluginName();

    Stats getPluginNodeStats();

    interface Stats extends ToXContent, NamedWriteable {}
}
