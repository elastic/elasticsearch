/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

/**
 * A {@link Runnable}-like interface which allows throwing checked exceptions.
 */
@FunctionalInterface
public interface CheckedRunnable<E extends Exception> {
    void run() throws E;
}
