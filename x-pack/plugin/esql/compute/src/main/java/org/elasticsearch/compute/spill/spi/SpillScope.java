/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill.spi;

import org.elasticsearch.action.ActionListener;

/**
 * One query's spilled data on one node, thread-safe. Everything written in a scope is deleted when the scope closes.
 */
public interface SpillScope {

    /**
     * A new writer, for one object.
     *
     * @param nameHint the operator and driver, for diagnostics only
     */
    void createWriter(String nameHint, ActionListener<SpillWriter> listener);

    /**
     * Requests that every outstanding operation of this scope stop. Returns at once; the operations complete their
     * listeners, with a failure where they did not finish, on their own. Safe to call at any time and more than once.
     */
    void cancel();

    /**
     * Deletes every object of the scope, finished or not, and completes when no worker of the backend touches the scope's
     * resources or borrowed memory any more. Outstanding operations are cancelled first. Idempotent: a second close
     * completes once the first has. An operation issued after the close fails with {@link IllegalStateException}.
     */
    void close(ActionListener<Void> listener);

    /**
     * Bytes this scope holds, a snapshot.
     */
    long bytesInUse();
}
