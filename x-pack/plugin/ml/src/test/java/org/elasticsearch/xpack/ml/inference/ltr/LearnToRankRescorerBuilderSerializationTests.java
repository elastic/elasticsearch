/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.ltr.MlLTRNamedXContentProvider;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.search.rank.RankBuilder.WINDOW_SIZE_FIELD;

public class LearnToRankRescorerBuilderSerializationTests extends AbstractBWCSerializationTestCase<LearnToRankRescorerBuilder> {

    @Override
    protected LearnToRankRescorerBuilder doParseInstance(XContentParser parser) throws IOException {
        String fieldName = null;
        LearnToRankRescorerBuilder rescorer = null;
        Integer windowSize = null;
        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    windowSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                rescorer = LearnToRankRescorerBuilder.fromXContent(parser, null);
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "] after [" + fieldName + "]");
            }
        }
        if (rescorer == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing rescore type");
        }
        if (windowSize != null) {
            rescorer.windowSize(windowSize);
        }
        return rescorer;
    }

    @Override
    protected Writeable.Reader<LearnToRankRescorerBuilder> instanceReader() {
        return in -> new LearnToRankRescorerBuilder(in, null);
    }

    @Override
    protected LearnToRankRescorerBuilder createTestInstance() {
        LearnToRankRescorerBuilder builder = randomBoolean()
            ? new LearnToRankRescorerBuilder(
                randomAlphaOfLength(10),
                randomBoolean() ? null : LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate(),
                null
            )
            : new LearnToRankRescorerBuilder(
                randomAlphaOfLength(10),
                LearnToRankConfigTests.randomLearnToRankConfig(),
                (Supplier<ModelLoadingService>) null
            );
        if (randomBoolean()) {
            builder.windowSize(randomIntBetween(1, 10000));
        }
        return builder;
    }

    @Override
    protected LearnToRankRescorerBuilder mutateInstance(LearnToRankRescorerBuilder instance) throws IOException {
        int i = randomInt(3);
        return switch (i) {
            case 0 -> {
                LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(
                    randomValueOtherThan(instance.getModelId(), () -> randomAlphaOfLength(10)),
                    instance.getInferenceConfigUpdate(),
                    null
                );
                if (instance.windowSize() != null) {
                    builder.windowSize(instance.windowSize());
                }
                yield builder;
            }
            case 1 -> new LearnToRankRescorerBuilder(instance.getModelId(), instance.getInferenceConfigUpdate(), null).windowSize(
                randomValueOtherThan(instance.windowSize(), () -> randomIntBetween(1, 10000))
            );
            case 2 -> {
                LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(
                    instance.getModelId(),
                    randomValueOtherThan(instance.getInferenceConfigUpdate(), LearnToRankConfigUpdateTests::randomLearnToRankConfigUpdate),
                    null
                );
                if (instance.windowSize() != null) {
                    builder.windowSize(instance.windowSize());
                }
                yield builder;
            }
            case 3 -> {
                LearnToRankRescorerBuilder builder = new LearnToRankRescorerBuilder(
                    instance.getModelId(),
                    randomValueOtherThan(instance.getInferenceConfig(), LearnToRankConfigTests::randomLearnToRankConfig),
                    (Supplier<ModelLoadingService>) null
                );
                if (instance.windowSize() != null) {
                    builder.windowSize(instance.windowSize());
                }
                yield builder;
            }
            default -> throw new AssertionError("Unexpected random test case");
        };
    }

    @Override
    protected LearnToRankRescorerBuilder mutateInstanceForVersion(LearnToRankRescorerBuilder instance, TransportVersion version) {
        return instance;
    }

    public void testIncorrectInferenceConfigUpdateType() {
        LearnToRankRescorerBuilder.Builder builder = new LearnToRankRescorerBuilder.Builder();
        expectThrows(
            IllegalArgumentException.class,
            () -> builder.setInferenceConfigUpdate(ClassificationConfigUpdateTests.randomClassificationConfigUpdate())
        );
        // Should not throw
        builder.setInferenceConfigUpdate(LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate());
    }

    public void testIncorrectInferenceConfigType() {
        LearnToRankRescorerBuilder.Builder builder = new LearnToRankRescorerBuilder.Builder();
        expectThrows(
            IllegalArgumentException.class,
            () -> builder.setInferenceConfig(ClassificationConfigTests.randomClassificationConfig())
        );
        // Should not throw
        builder.setInferenceConfig(LearnToRankConfigTests.randomLearnToRankConfig());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlLTRNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new MlLTRNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return writableRegistry();
    }
}
