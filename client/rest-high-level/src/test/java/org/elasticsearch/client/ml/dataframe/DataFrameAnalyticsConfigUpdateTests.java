/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataFrameAnalyticsConfigUpdateTests extends AbstractXContentTestCase<DataFrameAnalyticsConfigUpdate> {

    public static DataFrameAnalyticsConfigUpdate randomDataFrameAnalyticsConfigUpdate() {
        DataFrameAnalyticsConfigUpdate.Builder builder =
            DataFrameAnalyticsConfigUpdate.builder()
                .setId(randomAlphaOfLengthBetween(1, 10));
        if (randomBoolean()) {
            builder.setDescription(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            builder.setModelMemoryLimit(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            builder.setAllowLazyStart(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setMaxNumThreads(randomIntBetween(1, 20));
        }
        return builder.build();
    }

    @Override
    protected DataFrameAnalyticsConfigUpdate createTestInstance() {
        return randomDataFrameAnalyticsConfigUpdate();
    }

    @Override
    protected DataFrameAnalyticsConfigUpdate doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsConfigUpdate.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }
}
