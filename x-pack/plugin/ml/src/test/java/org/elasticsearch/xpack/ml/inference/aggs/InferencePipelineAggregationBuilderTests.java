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
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BasePipelineAggregationTestCase;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdate;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
            new InferencePipelineAggregationBuilder(NAME, new SetOnce<>(mock(ModelLoadingService.class)),
                mock(XPackLicenseState.class), bucketPaths);
        builder.setModelId(randomAlphaOfLength(6));

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

    public void testAdaptForAggregation_givenNull() {
        InferenceConfigUpdate update = InferencePipelineAggregationBuilder.adaptForAggregation(null);
        assertThat(update, is(instanceOf(ResultsFieldUpdate.class)));
        assertEquals(InferencePipelineAggregationBuilder.AGGREGATIONS_RESULTS_FIELD, update.getResultsField());
    }

    public void testAdaptForAggregation() {
        RegressionConfigUpdate regressionConfigUpdate = new RegressionConfigUpdate(null, 20);
        InferenceConfigUpdate update = InferencePipelineAggregationBuilder.adaptForAggregation(regressionConfigUpdate);
        assertEquals(InferencePipelineAggregationBuilder.AGGREGATIONS_RESULTS_FIELD, update.getResultsField());

        ClassificationConfigUpdate configUpdate = new ClassificationConfigUpdate(1, null, null, null, null);
        update = InferencePipelineAggregationBuilder.adaptForAggregation(configUpdate);
        assertEquals(InferencePipelineAggregationBuilder.AGGREGATIONS_RESULTS_FIELD, update.getResultsField());
    }

    public void testValidate() {
        InferencePipelineAggregationBuilder aggregationBuilder = createTestAggregatorFactory();
        PipelineAggregationBuilder.ValidationContext validationContext =
            PipelineAggregationBuilder.ValidationContext.forInsideTree(mock(AggregationBuilder.class), null);

        aggregationBuilder.setModelId(null);
        aggregationBuilder.validate(validationContext);
        List<String> errors = validationContext.getValidationException().validationErrors();
        assertEquals("[model_id] must be set", errors.get(0));
    }

    public void testValidate_invalidResultsField() {
        InferencePipelineAggregationBuilder aggregationBuilder = createTestAggregatorFactory();
        PipelineAggregationBuilder.ValidationContext validationContext =
            PipelineAggregationBuilder.ValidationContext.forInsideTree(mock(AggregationBuilder.class), null);

        RegressionConfigUpdate regressionConfigUpdate = new RegressionConfigUpdate("foo", null);
        aggregationBuilder.setInferenceConfig(regressionConfigUpdate);
        aggregationBuilder.validate(validationContext);
        List<String> errors = validationContext.getValidationException().validationErrors();
        assertEquals("setting option [results_field] to [foo] is not valid for inference aggregations", errors.get(0));
    }

    public void testValidate_invalidTopClassesField() {
        InferencePipelineAggregationBuilder aggregationBuilder = createTestAggregatorFactory();
        PipelineAggregationBuilder.ValidationContext validationContext =
            PipelineAggregationBuilder.ValidationContext.forInsideTree(mock(AggregationBuilder.class), null);

        ClassificationConfigUpdate configUpdate = new ClassificationConfigUpdate(1, null, "some_other_field", null, null);
        aggregationBuilder.setInferenceConfig(configUpdate);
        aggregationBuilder.validate(validationContext);
        List<String> errors = validationContext.getValidationException().validationErrors();
        assertEquals("setting option [top_classes] to [some_other_field] is not valid for inference aggregations", errors.get(0));
    }
}
