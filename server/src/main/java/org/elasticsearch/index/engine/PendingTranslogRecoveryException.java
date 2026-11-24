/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

/**
 * Thrown when a flush is requested while the engine still waits for its translog recovery. This is an expected state, so it has its own
 * type to let callers tell it apart from real flush failures.
 */
public class PendingTranslogRecoveryException extends IllegalStateException {

    public PendingTranslogRecoveryException(String message) {
        super(message);
    }
}
