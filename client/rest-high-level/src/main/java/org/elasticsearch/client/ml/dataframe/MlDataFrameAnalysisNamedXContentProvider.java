/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.Arrays;
import java.util.List;

public class MlDataFrameAnalysisNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(
                DataFrameAnalysis.class,
                OutlierDetection.NAME,
                (p, c) -> OutlierDetection.fromXContent(p)),
            new NamedXContentRegistry.Entry(
                DataFrameAnalysis.class,
                Regression.NAME,
                (p, c) -> Regression.fromXContent(p)),
            new NamedXContentRegistry.Entry(
                DataFrameAnalysis.class,
                Classification.NAME,
                (p, c) -> Classification.fromXContent(p)));
    }
}
