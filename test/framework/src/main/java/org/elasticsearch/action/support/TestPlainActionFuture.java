/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

/**
 * A {@link PlainActionFuture} which bypasses the deadlock-detection checks since we're only using this in tests.
 */
public class TestPlainActionFuture<T> extends PlainActionFuture<T> {
    @Override
    boolean allowedExecutors(Thread blockedThread, Thread completingThread) {
        return true;
    }
}
