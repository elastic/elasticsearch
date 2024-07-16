/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.elasticsearch.index.seqno.SequenceNumbers;

/**
 * Information about the safe commit, for making decisions about recoveries.
 */
public record SafeCommitInfo(long localCheckpoint, int docCount) {
    public static final SafeCommitInfo EMPTY = new SafeCommitInfo(SequenceNumbers.NO_OPS_PERFORMED, 0);
}
