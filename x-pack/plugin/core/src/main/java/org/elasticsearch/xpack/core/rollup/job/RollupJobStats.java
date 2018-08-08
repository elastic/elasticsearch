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
 * This class holds the runtime statistics of a job.  The stats are not used by any internal process
 * and are only for external monitoring/reference.  Statistics are not persisted with the job, so if the
 * allocated task is shutdown/restarted on a different node all the stats will reset.
 */

// todo: kept for now, consider removing it
public class RollupJobStats extends IndexerStats {

    public RollupJobStats(StreamInput in) throws IOException {
        super(in);
    }

    public RollupJobStats() {
        super();
    }

    
}

