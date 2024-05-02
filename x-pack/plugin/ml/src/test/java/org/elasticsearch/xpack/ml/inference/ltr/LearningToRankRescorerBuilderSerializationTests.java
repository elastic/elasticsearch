/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.ltr.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfigTests.randomLearningToRankConfig;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class LearningToRankRescorerBuilderSerializationTests extends AbstractBWCSerializationTestCase<LearningToRankRescorerBuilder> {

    private static LearningToRankService learningToRankService = mock(LearningToRankService.class);

    public void testRequiredWindowSize() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            LearningToRankRescorerBuilder testInstance = createTestInstance();
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.startObject();
                testInstance.doXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();

                try (XContentParser parser = JsonXContent.jsonXContent.createParser(parserConfig(), Strings.toString(builder))) {
                    ParsingException e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser, (r) -> {}));
                    assertThat(e.getMessage(), equalTo("window_size is required for rescorer of type [learning_to_rank]"));
                }
            }
        }
    }

    public void testModelIdIsRequired() throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder().startObject();
        if (randomBoolean()) {
            jsonBuilder.field("params", randomParams());
        }
        jsonBuilder.endObject();

        XContentParser parser = createParser(jsonBuilder);

        Exception e = assertThrows(
            IllegalArgumentException.class,
            () -> LearningToRankRescorerBuilder.fromXContent(parser, mock(LearningToRankService.class))
        );
        assertThat(e.getMessage(), containsString("Required one of fields [model_id], but none were specified."));
    }

    @Override
    protected LearningToRankRescorerBuilder doParseInstance(XContentParser parser) throws IOException {
        return (LearningToRankRescorerBuilder) RescorerBuilder.parseFromXContent(parser, (r) -> {});
    }

    @Override
    protected Writeable.Reader<LearningToRankRescorerBuilder> instanceReader() {
        return in -> new LearningToRankRescorerBuilder(in, learningToRankService);
    }

    @Override
    protected LearningToRankRescorerBuilder createTestInstance() {
        LearningToRankRescorerBuilder builder = randomBoolean()
            ? createXContextTestInstance(null)
            : new LearningToRankRescorerBuilder(
                randomAlphaOfLength(10),
                randomLearningToRankConfig(),
                randomBoolean() ? randomParams() : null,
                learningToRankService
            );

        builder.windowSize(randomIntBetween(1, 10000));

        return builder;
    }

    @Override
    protected LearningToRankRescorerBuilder createXContextTestInstance(XContentType xContentType) {
        return new LearningToRankRescorerBuilder(randomAlphaOfLength(10), randomBoolean() ? randomParams() : null, learningToRankService)
            .windowSize(randomIntBetween(1, 10000));
    }

    @Override
    protected LearningToRankRescorerBuilder mutateInstance(LearningToRankRescorerBuilder instance) throws IOException {
        int i = randomInt(4);
        return switch (i) {
            case 0 -> new LearningToRankRescorerBuilder(
                randomValueOtherThan(instance.modelId(), () -> randomAlphaOfLength(10)),
                instance.params(),
                learningToRankService
            ).windowSize(instance.windowSize());
            case 1 -> new LearningToRankRescorerBuilder(instance.modelId(), instance.params(), learningToRankService).windowSize(
                randomValueOtherThan(instance.windowSize(), () -> randomIntBetween(1, 10000))
            );
            case 2 -> new LearningToRankRescorerBuilder(
                instance.modelId(),
                randomValueOtherThan(instance.params(), () -> (randomBoolean() ? randomParams() : null)),
                learningToRankService
            ).windowSize(instance.windowSize());
            case 3 -> {
                LearningToRankConfig learningToRankConfig = randomValueOtherThan(
                    instance.learningToRankConfig(),
                    () -> randomLearningToRankConfig()
                );
                yield new LearningToRankRescorerBuilder(instance.modelId(), learningToRankConfig, null, learningToRankService).windowSize(
                    instance.windowSize()
                );
            }
            case 4 -> new LearningToRankRescorerBuilder(
                mock(LocalModel.class),
                instance.learningToRankConfig(),
                instance.params(),
                learningToRankService
            ).windowSize(instance.windowSize());
            default -> throw new AssertionError("Unexpected random test case");
        };
    }

    @Override
    protected LearningToRankRescorerBuilder mutateInstanceForVersion(LearningToRankRescorerBuilder instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                RescorerBuilder.class,
                LearningToRankRescorerBuilder.NAME,
                (p, c) -> LearningToRankRescorerBuilder.fromXContent(p, learningToRankService)
            )
        );
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return writableRegistry();
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new MlLTRNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                RescorerBuilder.class,
                LearningToRankRescorerBuilder.NAME.getPreferredName(),
                in -> new LearningToRankRescorerBuilder(in, learningToRankService)
            )
        );
        return new NamedWriteableRegistry(namedWriteables);
    }

    private static Map<String, Object> randomParams() {
        return randomMap(1, randomIntBetween(1, 10), () -> new Tuple<>(randomIdentifier(), randomIdentifier()));
    }
}
