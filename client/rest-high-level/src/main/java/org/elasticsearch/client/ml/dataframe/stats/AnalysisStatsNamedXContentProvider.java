/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats;

import org.elasticsearch.client.ml.dataframe.stats.classification.ClassificationStats;
import org.elasticsearch.client.ml.dataframe.stats.outlierdetection.OutlierDetectionStats;
import org.elasticsearch.client.ml.dataframe.stats.regression.RegressionStats;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.Arrays;
import java.util.List;

public class AnalysisStatsNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(
                AnalysisStats.class,
                ClassificationStats.NAME,
                (p, c) -> ClassificationStats.PARSER.apply(p, null)
            ),
            new NamedXContentRegistry.Entry(
                AnalysisStats.class,
                OutlierDetectionStats.NAME,
                (p, c) -> OutlierDetectionStats.PARSER.apply(p, null)
            ),
            new NamedXContentRegistry.Entry(
                AnalysisStats.class,
                RegressionStats.NAME,
                (p, c) -> RegressionStats.PARSER.apply(p, null)
            )
        );
    }
}
