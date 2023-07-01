/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInputTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigTests;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class TransportPutTrainedModelActionTests extends ESTestCase {

    public void testParseInferenceConfigFromModelPackage() throws IOException {

        InferenceConfig inferenceConfig = randomFrom(
            new InferenceConfig[] {
                ClassificationConfigTests.randomClassificationConfig(),
                RegressionConfigTests.randomRegressionConfig(),
                NerConfigTests.createRandom(),
                PassThroughConfigTests.createRandom(),
                TextClassificationConfigTests.createRandom(),
                FillMaskConfigTests.createRandom(),
                TextEmbeddingConfigTests.createRandom(),
                QuestionAnsweringConfigTests.createRandom(),
                TextSimilarityConfigTests.createRandom(),
                TextExpansionConfigTests.createRandom() }
        );

        Map<String, Object> inferenceConfigMap;
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = inferenceConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            inferenceConfigMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
        }

        assertNotNull(inferenceConfigMap);
        InferenceConfig parsedInferenceConfig = TransportPutTrainedModelAction.parseInferenceConfigFromModelPackage(
            Collections.singletonMap(inferenceConfig.getWriteableName(), inferenceConfigMap),
            xContentRegistry(),
            LoggingDeprecationHandler.INSTANCE
        );

        assertEquals(inferenceConfig, parsedInferenceConfig);
    }

    public void testSetTrainedModelConfigFieldsFromPackagedModel() throws IOException {
        ModelPackageConfig packageConfig = ModelPackageConfigTests.randomModulePackageConfig();

        TrainedModelConfig.Builder trainedModelConfigBuilder = new TrainedModelConfig.Builder().setModelId(
            "." + packageConfig.getPackagedModelId()
        ).setInput(TrainedModelInputTests.createRandomInput());

        TransportPutTrainedModelAction.setTrainedModelConfigFieldsFromPackagedModel(
            trainedModelConfigBuilder,
            packageConfig,
            xContentRegistry()
        );

        TrainedModelConfig trainedModelConfig = trainedModelConfigBuilder.build();

        assertEquals(packageConfig.getModelType(), trainedModelConfig.getModelType().toString());
        assertEquals(packageConfig.getDescription(), trainedModelConfig.getDescription());
        assertEquals(packageConfig.getMetadata(), trainedModelConfig.getMetadata());
        assertEquals(packageConfig.getTags(), trainedModelConfig.getTags());

        // fully tested in {@link #testParseInferenceConfigFromModelPackage}
        assertNotNull(trainedModelConfig.getInferenceConfig());

        assertEquals(
            TrainedModelType.fromString(packageConfig.getModelType()).getDefaultLocation(trainedModelConfig.getModelId()),
            trainedModelConfig.getLocation()
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
