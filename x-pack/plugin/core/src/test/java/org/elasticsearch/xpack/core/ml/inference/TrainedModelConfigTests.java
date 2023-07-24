/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.license.License;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.IndexLocationTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigTests;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TrainedModelConfigTests extends AbstractBWCSerializationTestCase<TrainedModelConfig> {

    private boolean lenient;

    public static TrainedModelConfig.Builder createTestInstance(String modelId) {
        return createTestInstance(modelId, false);
    }

    public static TrainedModelConfig.Builder createTestInstance(String modelId, boolean lenient) {

        InferenceConfig[] inferenceConfigs = lenient ?
        // Because of vocab config validations on parse, only test on lenient
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
            : new InferenceConfig[] {
                ClassificationConfigTests.randomClassificationConfig(),
                RegressionConfigTests.randomRegressionConfig() };
        List<String> tags = Arrays.asList(generateRandomStringArray(randomIntBetween(0, 5), 15, false));
        return TrainedModelConfig.builder()
            .setInput(TrainedModelInputTests.createRandomInput())
            .setMetadata(randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)))
            .setCreateTime(Instant.ofEpochMilli(randomLongBetween(Instant.MIN.getEpochSecond(), Instant.MAX.getEpochSecond())))
            .setVersion(Version.CURRENT)
            .setModelId(modelId)
            .setModelType(randomFrom(TrainedModelType.values()))
            .setCreatedBy(randomAlphaOfLength(10))
            .setDescription(randomBoolean() ? null : randomAlphaOfLength(10))
            .setModelSize(randomNonNegativeLong())
            .setEstimatedOperations(randomNonNegativeLong())
            .setLicenseLevel(randomFrom(License.OperationMode.PLATINUM.description(), License.OperationMode.BASIC.description()))
            .setInferenceConfig(randomFrom(inferenceConfigs))
            .setTags(tags)
            .setLocation(randomBoolean() ? null : IndexLocationTests.randomInstance());
    }

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected TrainedModelConfig doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelConfig.fromXContent(parser, lenient).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected TrainedModelConfig createTestInstance() {
        return createTestInstance(randomAlphaOfLength(10), lenient).build();
    }

    @Override
    protected TrainedModelConfig mutateInstance(TrainedModelConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<TrainedModelConfig> instanceReader() {
        return TrainedModelConfig::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return lenient ? ToXContent.EMPTY_PARAMS : new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    public void testToXContentWithParams() throws IOException {
        TrainedModelConfig.LazyModelDefinition lazyModelDefinition = TrainedModelConfig.LazyModelDefinition.fromParsedDefinition(
            TrainedModelDefinitionTests.createRandomBuilder().build()
        );
        TrainedModelConfig config = new TrainedModelConfig(
            randomAlphaOfLength(10),
            TrainedModelType.TREE_ENSEMBLE,
            randomAlphaOfLength(10),
            Version.CURRENT,
            randomBoolean() ? null : randomAlphaOfLength(100),
            Instant.ofEpochMilli(randomNonNegativeLong()),
            lazyModelDefinition,
            Collections.emptyList(),
            randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            TrainedModelInputTests.createRandomInput(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            "platinum",
            randomBoolean()
                ? null
                : Stream.generate(() -> randomAlphaOfLength(10))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toMap(Function.identity(), (k) -> randomAlphaOfLength(10))),
            randomFrom(ClassificationConfigTests.randomClassificationConfig(), RegressionConfigTests.randomRegressionConfig()),
            null,
            ModelPackageConfigTests.randomModulePackageConfig()
        );

        BytesReference reference = XContentHelper.toXContent(config, XContentType.JSON, ToXContent.EMPTY_PARAMS, false);
        assertThat(reference.utf8ToString(), containsString("\"compressed_definition\""));

        reference = XContentHelper.toXContent(
            config,
            XContentType.JSON,
            new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")),
            false
        );
        assertThat(reference.utf8ToString(), not(containsString("definition")));
        assertThat(reference.utf8ToString(), not(containsString("compressed_definition")));

        reference = XContentHelper.toXContent(
            config,
            XContentType.JSON,
            new ToXContent.MapParams(Collections.singletonMap(TrainedModelConfig.DECOMPRESS_DEFINITION, "true")),
            false
        );
        assertThat(reference.utf8ToString(), containsString("\"definition\""));
        assertThat(reference.utf8ToString(), not(containsString("compressed_definition")));
    }

    public void testParseWithBothDefinitionAndCompressedSupplied() throws IOException {
        TrainedModelConfig.LazyModelDefinition lazyModelDefinition = TrainedModelConfig.LazyModelDefinition.fromParsedDefinition(
            TrainedModelDefinitionTests.createRandomBuilder().build()
        );
        TrainedModelConfig config = new TrainedModelConfig(
            randomAlphaOfLength(10),
            TrainedModelType.TREE_ENSEMBLE,
            randomAlphaOfLength(10),
            Version.CURRENT,
            randomBoolean() ? null : randomAlphaOfLength(100),
            Instant.ofEpochMilli(randomNonNegativeLong()),
            lazyModelDefinition,
            Collections.emptyList(),
            randomBoolean() ? null : Collections.singletonMap(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            TrainedModelInputTests.createRandomInput(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            "platinum",
            randomBoolean()
                ? null
                : Stream.generate(() -> randomAlphaOfLength(10))
                    .limit(randomIntBetween(1, 10))
                    .collect(Collectors.toMap(Function.identity(), (k) -> randomAlphaOfLength(10))),
            randomFrom(ClassificationConfigTests.randomClassificationConfig(), RegressionConfigTests.randomRegressionConfig()),
            null,
            ModelPackageConfigTests.randomModulePackageConfig()
        );

        BytesReference reference = XContentHelper.toXContent(config, XContentType.JSON, ToXContent.EMPTY_PARAMS, false);
        Map<String, Object> objectMap = XContentHelper.convertToMap(reference, true, XContentType.JSON).v2();

        objectMap.put(TrainedModelConfig.DEFINITION.getPreferredName(), config.getModelDefinition());

        try (
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(objectMap);
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    xContentRegistry(),
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    BytesReference.bytes(xContentBuilder).streamInput()
                )
        ) {
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> TrainedModelConfig.fromXContent(parser, true));
            assertThat(ex.getCause().getMessage(), equalTo("both [compressed_definition] and [definition] cannot be set."));
        }
    }

    public void testValidateWithBothDefinitionAndLocation() {
        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder()
                .setLocation(IndexLocationTests.randomInstance())
                .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
                .setModelType(TrainedModelType.PYTORCH)
                .validate()
        );
        assertThat(ex.getMessage(), containsString("[definition] and [location] are both defined but only one can be used."));
    }

    public void testValidateWithWithMissingTypeAndDefinition() {
        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder().setLocation(IndexLocationTests.randomInstance()).validate()
        );
        assertThat(ex.getMessage(), containsString("[model_type] must be set if [definition] is not defined"));
    }

    public void testValidateWithInvalidID() {
        String modelId = "InvalidID-";
        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder()
                .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
                .setModelId(modelId)
                .validate()
        );
        assertThat(ex.getMessage(), containsString(Messages.getMessage(Messages.INVALID_ID, "model_id", modelId)));
    }

    public void testValidateWithLongID() {
        String modelId = IntStream.range(0, 100).mapToObj(x -> "a").collect(Collectors.joining());
        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder()
                .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
                .setModelId(modelId)
                .validate()
        );
        assertThat(
            ex.getMessage(),
            containsString(Messages.getMessage(Messages.ID_TOO_LONG, "model_id", modelId, MlStrings.ID_LENGTH_LIMIT))
        );
    }

    public void testValidateWithIllegallyUserProvidedFields() {
        String modelId = "simplemodel";
        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder()
                .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
                .setCreateTime(Instant.now())
                .setModelId(modelId)
                .validate(true)
        );
        assertThat(ex.getMessage(), containsString("illegal to set [create_time] at inference model creation"));

        ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder()
                .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
                .setVersion(Version.CURRENT)
                .setModelId(modelId)
                .validate(true)
        );
        assertThat(ex.getMessage(), containsString("illegal to set [version] at inference model creation"));

        ex = expectThrows(
            ActionRequestValidationException.class,
            () -> TrainedModelConfig.builder()
                .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
                .setCreatedBy("ml_user")
                .setModelId(modelId)
                .validate(true)
        );
        assertThat(ex.getMessage(), containsString("illegal to set [created_by] at inference model creation"));
    }

    public void testSerializationWithLazyDefinition() throws IOException {
        xContentTester(this::createParser, () -> {
            try {
                BytesReference bytes = InferenceToXContentCompressor.deflate(TrainedModelDefinitionTests.createRandomBuilder().build());
                return createTestInstance(randomAlphaOfLength(10), lenient).setDefinitionFromBytes(bytes).build();
            } catch (IOException ex) {
                fail(ex.getMessage());
                return null;
            }
        }, ToXContent.EMPTY_PARAMS, (p) -> TrainedModelConfig.fromXContent(p, true).build()).numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(false)
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer((def1, def2) -> {
                try {
                    assertThat(
                        def1.ensureParsedDefinition(xContentRegistry()).getModelDefinition(),
                        equalTo(def2.ensureParsedDefinition(xContentRegistry()).getModelDefinition())
                    );
                } catch (IOException ex) {
                    fail(ex.getMessage());
                }
            })
            .assertToXContentEquivalence(true)
            .test();
    }

    public void testSerializationWithCompressedLazyDefinition() throws IOException {
        xContentTester(this::createParser, () -> {
            try {
                BytesReference bytes = InferenceToXContentCompressor.deflate(TrainedModelDefinitionTests.createRandomBuilder().build());
                return createTestInstance(randomAlphaOfLength(10), lenient).setDefinitionFromBytes(bytes).build();
            } catch (IOException ex) {
                fail(ex.getMessage());
                return null;
            }
        },
            new ToXContent.MapParams(Collections.singletonMap(TrainedModelConfig.DECOMPRESS_DEFINITION, "false")),
            (p) -> TrainedModelConfig.fromXContent(p, true).build()
        ).numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(false)
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer((def1, def2) -> {
                try {
                    assertThat(
                        def1.ensureParsedDefinition(xContentRegistry()).getModelDefinition(),
                        equalTo(def2.ensureParsedDefinition(xContentRegistry()).getModelDefinition())
                    );
                } catch (IOException ex) {
                    fail(ex.getMessage());
                }
            })
            .assertToXContentEquivalence(true)
            .test();
    }

    public void testValidatePackagedModelRequiredFields() {
        String modelId = "." + randomAlphaOfLength(20).toLowerCase(Locale.ROOT);

        TrainedModelConfig.Builder builder = TrainedModelConfig.builder().setModelId(modelId);

        // all fine
        assertNotNull(builder.validate(true));
        assertNotNull(builder.validateNoPackageOverrides());

        String field = "";
        switch (randomIntBetween(0, 4)) {
            case 0:
                builder.setDescription(randomAlphaOfLength(10));
                field = "description";
                break;
            case 1:
                builder.setModelType(TrainedModelType.PYTORCH);
                field = "model_type";
                break;
            case 2:
                builder.setMetadata(Collections.singletonMap("meta", "data"));
                field = "metadata";
                break;
            case 3:
                builder.setTags(List.of("tag1", "tag2"));
                field = "tags";
                break;
            case 4:
                builder.setInferenceConfig(
                    randomFrom(ClassificationConfigTests.randomClassificationConfig(), RegressionConfigTests.randomRegressionConfig())
                );
                field = "inference_config";
                break;
        }

        ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            "expected to throw for field: " + field,
            () -> builder.validateNoPackageOverrides()
        );
        assertThat(ex.getMessage(), containsString("illegal to set [" + field + "] at inference model creation for packaged model;"));
    }

    @Override
    protected TrainedModelConfig mutateInstanceForVersion(TrainedModelConfig instance, TransportVersion version) {
        TrainedModelConfig.Builder builder = new TrainedModelConfig.Builder(instance);
        if (version.before(TrainedModelConfig.VERSION_3RD_PARTY_CONFIG_ADDED)) {
            builder.setModelType(null);
            builder.setLocation(null);
        }
        if (instance.getInferenceConfig() instanceof NlpConfig nlpConfig) {
            builder.setInferenceConfig(InferenceConfigItemTestCase.mutateForVersion(nlpConfig, version));
        }
        return builder.build();
    }
}
