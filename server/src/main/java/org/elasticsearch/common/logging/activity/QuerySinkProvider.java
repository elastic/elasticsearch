/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import java.util.List;

/**
 * SPI for plugins contributing query sinks. Register implementations in
 * {@code META-INF/services/org.elasticsearch.common.logging.activity.QuerySinkProvider}.
 * Providers may have a public no-argument constructor or a public constructor accepting their plugin instance.
 */
public interface QuerySinkProvider {
    /**
     * Creates sinks once per node. The returned sinks are shared across query types and may be called concurrently.
     * Plugins own any resources used by their sinks and must release them when the plugin closes.
     */
    List<QuerySink<QueryCaptureContext>> getQuerySinks();
}
