/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.stats;

import org.elasticsearch.common.metrics.DoubleMean;

public class ShardWriteLoadStats {
    private final DoubleMean writeLoadMean;

    public ShardWriteLoadStats(DoubleMean writeLoadMean) {
        this.writeLoadMean = writeLoadMean;
    }

    public double indexingLoadAvg() {
        return writeLoadMean.mean();
    }
}
