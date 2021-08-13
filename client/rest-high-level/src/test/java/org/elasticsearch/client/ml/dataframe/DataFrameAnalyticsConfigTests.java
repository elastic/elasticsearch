/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.Version;
import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsDestTests.randomDestConfig;
import static org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsSourceTests.randomSourceConfig;
import static org.elasticsearch.client.ml.dataframe.OutlierDetectionTests.randomOutlierDetection;

public class DataFrameAnalyticsConfigTests extends AbstractXContentTestCase<DataFrameAnalyticsConfig> {

    public static DataFrameAnalyticsConfig randomDataFrameAnalyticsConfig() {
        DataFrameAnalyticsConfig.Builder builder =
            DataFrameAnalyticsConfig.builder()
                .setId(randomAlphaOfLengthBetween(1, 10))
                .setSource(randomSourceConfig())
                .setDest(randomDestConfig())
                .setAnalysis(randomOutlierDetection());
        if (randomBoolean()) {
            builder.setDescription(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            builder.setAnalyzedFields(new FetchSourceContext(true,
                generateRandomStringArray(10, 10, false, false),
                generateRandomStringArray(10, 10, false, false)));
        }
        if (randomBoolean()) {
            builder.setModelMemoryLimit(new ByteSizeValue(randomIntBetween(1, 16), randomFrom(ByteSizeUnit.MB, ByteSizeUnit.GB)));
        }
        if (randomBoolean()) {
            builder.setCreateTime(Instant.now());
        }
        if (randomBoolean()) {
            builder.setVersion(Version.CURRENT);
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
    protected DataFrameAnalyticsConfig createTestInstance() {
        return randomDataFrameAnalyticsConfig();
    }

    @Override
    protected DataFrameAnalyticsConfig doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsConfig.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
    }
}
