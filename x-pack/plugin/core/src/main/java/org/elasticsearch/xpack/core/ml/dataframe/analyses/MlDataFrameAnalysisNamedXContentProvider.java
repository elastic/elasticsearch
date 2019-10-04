/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.ArrayList;
import java.util.List;

public class MlDataFrameAnalysisNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();

        namedXContent.add(new NamedXContentRegistry.Entry(DataFrameAnalysis.class, OutlierDetection.NAME, (p, c) -> {
            boolean ignoreUnknownFields = (boolean) c;
            return OutlierDetection.fromXContent(p, ignoreUnknownFields);
        }));
        namedXContent.add(new NamedXContentRegistry.Entry(DataFrameAnalysis.class, Regression.NAME, (p, c) -> {
            boolean ignoreUnknownFields = (boolean) c;
            return Regression.fromXContent(p, ignoreUnknownFields);
        }));

        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        namedWriteables.add(new NamedWriteableRegistry.Entry(DataFrameAnalysis.class, OutlierDetection.NAME.getPreferredName(),
            OutlierDetection::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(DataFrameAnalysis.class, Regression.NAME.getPreferredName(),
            Regression::new));

        return namedWriteables;
    }
}
