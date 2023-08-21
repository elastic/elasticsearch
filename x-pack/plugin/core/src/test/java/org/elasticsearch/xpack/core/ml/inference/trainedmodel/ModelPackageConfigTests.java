/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ModelPackageConfigTests extends AbstractBWCSerializationTestCase<ModelPackageConfig> {

    public static ModelPackageConfig randomModulePackageConfig() {
        return new ModelPackageConfig(
            randomAlphaOfLength(10),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean()
                ? Instant.ofEpochMilli(randomLongBetween(Instant.MIN.getEpochSecond(), Instant.MAX.getEpochSecond() - 100))
                : null,
            randomLongBetween(0, Long.MAX_VALUE - 100),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomInferenceConfigAsMap(),
            randomBoolean() ? Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null,
            randomFrom(TrainedModelType.values()).toString(),
            randomBoolean() ? Arrays.asList(generateRandomStringArray(randomIntBetween(0, 5), 15, false)) : null,
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

    public static ModelPackageConfig mutateModelPackageConfig(ModelPackageConfig instance) {
        switch (between(0, 11)) {
            case 0:
                return new ModelPackageConfig.Builder(instance).setPackedModelId(randomAlphaOfLength(15)).build();
            case 1:
                return new ModelPackageConfig.Builder(instance).setModelRepository(randomAlphaOfLength(15)).build();
            case 2:
                return new ModelPackageConfig.Builder(instance).setDescription(randomAlphaOfLength(15)).build();
            case 3:
                return new ModelPackageConfig.Builder(instance).setMinimumVersion(randomAlphaOfLength(15)).build();
            case 4:
                return new ModelPackageConfig.Builder(instance).setCreateTime(
                    instance.getCreateTime() == null
                        ? Instant.ofEpochMilli(randomLongBetween(Instant.MIN.getEpochSecond(), Instant.MAX.getEpochSecond()))
                        : instance.getCreateTime().plus(Duration.ofMillis(randomIntBetween(1, 100)))
                ).build();
            case 5:
                return new ModelPackageConfig.Builder(instance).setSize(instance.getSize() + randomLongBetween(1, 100)).build();
            case 6:
                return new ModelPackageConfig.Builder(instance).setSha256(randomAlphaOfLength(15)).build();
            case 7:
                return new ModelPackageConfig.Builder(instance).setInferenceConfigSource(
                    Collections.singletonMap(randomAlphaOfLength(15), randomAlphaOfLength(15))
                ).build();
            case 8:
                return new ModelPackageConfig.Builder(instance).setMetadata(
                    Collections.singletonMap(randomAlphaOfLength(15), randomAlphaOfLength(15))
                ).build();
            case 9:
                return new ModelPackageConfig.Builder(instance).setModelType(randomAlphaOfLength(15)).build();
            case 10:
                return new ModelPackageConfig.Builder(instance).setTags(
                    Arrays.asList(generateRandomStringArray(randomIntBetween(1, 5), 20, false, false))
                ).build();
            case 11:
                return new ModelPackageConfig.Builder(instance).setVocabularyFile(randomAlphaOfLength(15)).build();
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
    }

    @Override
    protected ModelPackageConfig doParseInstance(XContentParser parser) throws IOException {
        return ModelPackageConfig.fromXContentLenient(parser);
    }

    @Override
    protected Writeable.Reader<ModelPackageConfig> instanceReader() {
        return ModelPackageConfig::new;
    }

    @Override
    protected ModelPackageConfig createTestInstance() {
        return randomModulePackageConfig();
    }

    @Override
    protected ModelPackageConfig mutateInstance(ModelPackageConfig instance) {
        return mutateModelPackageConfig(instance);
    }

    @Override
    protected ModelPackageConfig mutateInstanceForVersion(ModelPackageConfig instance, TransportVersion version) {
        return instance;
    }

    private static Map<String, Object> randomInferenceConfigAsMap() {
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

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = inferenceConfig.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return Collections.singletonMap(
                inferenceConfig.getWriteableName(),
                XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
