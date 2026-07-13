/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.Set;

public interface IndexEngineLocalReaderListener {
    /**
     * Listener invoked when one of the local readers, which holds a commit, is closed.
     * @param bccHoldingClosedCommit the bcc generation that contains the closed commit
     * @param remainingReferencedBCCs set of the remaining held BCCs by other local readers
     */
    void onLocalReaderClosed(long bccHoldingClosedCommit, Set<PrimaryTermAndGeneration> remainingReferencedBCCs);
}
