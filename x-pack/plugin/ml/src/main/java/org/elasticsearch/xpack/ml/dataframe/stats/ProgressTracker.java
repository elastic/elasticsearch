/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.stats;

import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ProgressTracker {

    public static final String REINDEXING = "reindexing";
    public static final String LOADING_DATA = "loading_data";
    public static final String ANALYZING = "analyzing";
    public static final String WRITING_RESULTS = "writing_results";

    public final AtomicInteger reindexingPercent = new AtomicInteger(0);
    public final AtomicInteger loadingDataPercent = new AtomicInteger(0);
    public final AtomicInteger analyzingPercent = new AtomicInteger(0);
    public final AtomicInteger writingResultsPercent = new AtomicInteger(0);

    public List<PhaseProgress> report() {
        return Arrays.asList(
            new PhaseProgress(REINDEXING, reindexingPercent.get()),
            new PhaseProgress(LOADING_DATA, loadingDataPercent.get()),
            new PhaseProgress(ANALYZING, analyzingPercent.get()),
            new PhaseProgress(WRITING_RESULTS, writingResultsPercent.get())
        );
    }
}
