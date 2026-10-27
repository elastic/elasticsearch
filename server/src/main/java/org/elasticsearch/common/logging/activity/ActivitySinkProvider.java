/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

/**
 * Extension point that lets plugins add an {@link ActivitySink} to the {@link ActivityInterceptor} of an action, independently of
 * the query log. Unlike {@link ActivityLogWriterProvider} there can be any number of them.
 * <p>
 * Implementations are discovered as SPI services from plugins. They are created before the plugin's components, so the sink
 * must report itself as not enabled until it is ready.
 *
 * @param <C> the logging context type of the action the sink handles
 */
public interface ActivitySinkProvider<C extends ActivityCaptureContext> {

    /**
     * The context type of the actions whose logger gets the sink, e.g. {@code SearchCaptureContext.class}. A sink for one type
     * is not given to the loggers of other actions.
     */
    Class<C> contextType();

    /**
     * The sink to add. Called once for each logger it is added to, so it should return the same instance.
     */
    ActivitySink<C, ?> sink();
}
