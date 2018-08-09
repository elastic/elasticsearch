/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.indexing.IndexerStats;

import java.io.IOException;

/**
 * This class holds the runtime statistics of a rollup job, derived from {@link IndexerStats}}.
 *
 */
public class RollupJobStats extends IndexerStats {

    public RollupJobStats(StreamInput in) throws IOException {
        super(in);
    }

    public RollupJobStats() {
        super();
    }

    public RollupJobStats(long numPages, long numDocuments, long numRollups, long numInvocations) {
        super (numPages, numDocuments, numRollups, numInvocations);
    }
}

