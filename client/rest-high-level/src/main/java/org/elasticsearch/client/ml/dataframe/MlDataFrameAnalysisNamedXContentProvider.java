/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.ArrayList;
import java.util.List;

public class MlDataFrameAnalysisNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();

        namedXContent.add(new NamedXContentRegistry.Entry(DataFrameAnalysis.class, OutlierDetection.NAME, (p, c) -> {
            return OutlierDetection.fromXContent(p);
        }));

        return namedXContent;
    }
}
