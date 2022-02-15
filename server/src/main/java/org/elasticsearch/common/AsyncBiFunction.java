/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import org.elasticsearch.action.ActionListener;

/**
 * A {@link java.util.function.BiFunction}-like interface designed to be used with asynchronous executions.
 */
public interface AsyncBiFunction<T, U, C> {

    void apply(T t, U u, ActionListener<C> listener);
}
