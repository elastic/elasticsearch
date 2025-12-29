/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.action;

/**
 * Builder class for a logger context.
 * The builder is created at the beginning of a logging operation and will produce a context when the operation is completed,
 * which is usually the completion of the listener.
 * @param <Context> Context type to build.
 * @param <R> Response type for the listener.
 */
public interface ActionLoggerContextBuilder<Context extends ActionLoggerContext, R> {
    /**
     * Build context for successful completion
     */
    Context build(R response);

    /**
     * Build context for failure completion
     */
    Context build(Exception e);
}
