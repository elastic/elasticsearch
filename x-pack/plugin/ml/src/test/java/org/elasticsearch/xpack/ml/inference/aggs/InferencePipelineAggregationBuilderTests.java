/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.aggs;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdateTests;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class InferencePipelineAggregationBuilderTests extends BasePipelineAggregationTestCase<InferencePipelineAggregationBuilder> {

    private static final String NAME = "inf-agg";

    @Override
    protected List<SearchPlugin> plugins() {
        return Collections.singletonList(new MachineLearning(Settings.EMPTY, null));
    }

    @Override
    protected List<NamedXContentRegistry.Entry> additionalNamedContents() {
        return new MlInferenceNamedXContentProvider().getNamedXContentParsers();
    }

    @Override
    protected List<NamedWriteableRegistry.Entry> additionalNamedWriteables() {
        return new MlInferenceNamedXContentProvider().getNamedWriteables();
    }

    @Override
    protected InferencePipelineAggregationBuilder createTestAggregatorFactory() {
        Map<String, String> bucketPaths = Stream.generate(() -> randomAlphaOfLength(8))
            .limit(randomIntBetween(1, 4))
            .collect(Collectors.toMap(Function.identity(), (t) -> randomAlphaOfLength(5)));

        InferencePipelineAggregationBuilder builder =
            new InferencePipelineAggregationBuilder(NAME, new SetOnce<>(mock(ModelLoadingService.class)), bucketPaths);
        builder.setModelId(randomAlphaOfLength(6));

        if (randomBoolean()) {
            builder.setGapPolicy(randomFrom(BucketHelpers.GapPolicy.values()));
        }
        if (randomBoolean()) {
            InferenceConfigUpdate config;
            if (randomBoolean()) {
                config = ClassificationConfigUpdateTests.randomClassificationConfigUpdate();
            } else {
                config = RegressionConfigUpdateTests.randomRegressionConfigUpdate();
            }
            builder.setInferenceConfig(config);
        }
        return builder;
    }
}
