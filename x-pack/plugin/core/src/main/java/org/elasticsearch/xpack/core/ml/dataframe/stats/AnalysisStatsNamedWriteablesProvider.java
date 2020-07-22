/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStats;

import java.util.Arrays;
import java.util.List;

public class AnalysisStatsNamedWriteablesProvider {

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(AnalysisStats.class, ClassificationStats.TYPE_VALUE, ClassificationStats::new),
            new NamedWriteableRegistry.Entry(AnalysisStats.class, OutlierDetectionStats.TYPE_VALUE, OutlierDetectionStats::new),
            new NamedWriteableRegistry.Entry(AnalysisStats.class, RegressionStats.TYPE_VALUE, RegressionStats::new)
        );
    }
}
