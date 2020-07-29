/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.stats.classification.ClassificationStatsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsageTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.outlierdetection.OutlierDetectionStatsTests;
import org.elasticsearch.xpack.core.ml.dataframe.stats.regression.RegressionStatsTests;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.inference.modelsize.MlModelSizeNamedXContentProvider;
import org.elasticsearch.xpack.ml.inference.modelsize.ModelSizeInfoTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AnalyticsResultTests extends AbstractXContentTestCase<AnalyticsResult> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlModelSizeNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    protected AnalyticsResult createTestInstance() {
        AnalyticsResult.Builder builder = AnalyticsResult.builder();

        if (randomBoolean()) {
            builder.setRowResults(RowResultsTests.createRandom());
        }
        if (randomBoolean()) {
            builder.setPhaseProgress(new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100)));
        }
        if (randomBoolean()) {
            builder.setMemoryUsage(MemoryUsageTests.createRandom());
        }
        if (randomBoolean()) {
            builder.setOutlierDetectionStats(OutlierDetectionStatsTests.createRandom());
        }
        if (randomBoolean()) {
            builder.setClassificationStats(ClassificationStatsTests.createRandom());
        }
        if (randomBoolean()) {
            builder.setRegressionStats(RegressionStatsTests.createRandom());
        }
        if (randomBoolean()) {
            builder.setModelSizeInfo(ModelSizeInfoTests.createRandom());
        }
        if (randomBoolean()) {
            String def = randomAlphaOfLengthBetween(100, 1000);
            builder.setTrainedModelDefinitionChunk(new TrainedModelDefinitionChunk(def, randomIntBetween(0, 10), randomBoolean()));
        }
        return builder.build();
    }

    @Override
    protected AnalyticsResult doParseInstance(XContentParser parser) {
        return AnalyticsResult.PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
