/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.elasticsearch.benchmark.esql.ViewResolutionBenchmarkBase;
import org.openjdk.jmh.annotations.Param;

/**
 * Nightly regression subset of the view resolution benchmark.
 * <p>
 * Runs a targeted set of scenarios (5 combos x 2 methods = 10 benchmarks)
 * to detect regressions without the full hour-long sweep. For the complete
 * parameter matrix, use {@link org.elasticsearch.benchmark.esql.ViewResolutionFullBenchmark}.
 */
public class ViewResolutionBenchmark extends ViewResolutionBenchmarkBase {

    @Param({ "disabled", "enabled_no_views", "enabled_index", "depth_1", "depth_3_filter" })
    String scenario;

    @Param({ "false" })
    String queryFilter;

    @Override
    protected String getScenario() {
        return scenario;
    }

    @Override
    protected String getQueryFilter() {
        return queryFilter;
    }
}
