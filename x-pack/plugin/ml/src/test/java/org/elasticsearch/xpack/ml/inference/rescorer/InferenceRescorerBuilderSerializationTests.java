/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdateTests;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.search.rank.RankBuilder.WINDOW_SIZE_FIELD;

public class InferenceRescorerBuilderSerializationTests extends AbstractBWCSerializationTestCase<InferenceRescorerBuilder> {

    @Override
    protected InferenceRescorerBuilder doParseInstance(XContentParser parser) throws IOException {
        String fieldName = null;
        InferenceRescorerBuilder rescorer = null;
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
                rescorer = InferenceRescorerBuilder.fromXContent(parser, null);
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
    protected Writeable.Reader<InferenceRescorerBuilder> instanceReader() {
        return in -> new InferenceRescorerBuilder(in, null);
    }

    @Override
    protected InferenceRescorerBuilder createTestInstance() {
        InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
            randomAlphaOfLength(10),
            randomBoolean() ? null : LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate(),
            (Supplier<ModelLoadingService>) null
        );
        if (randomBoolean()) {
            builder.windowSize(randomIntBetween(1, 10000));
        }
        return builder;
    }

    @Override
    protected InferenceRescorerBuilder mutateInstance(InferenceRescorerBuilder instance) throws IOException {
        int i = randomInt(2);
        return switch (i) {
            case 0 -> {
                InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
                    randomValueOtherThan(instance.getModelId(), () -> randomAlphaOfLength(10)),
                    instance.getInferenceConfigUpdate(),
                    (Supplier<ModelLoadingService>) null);
                if (instance.windowSize() != null) {
                    builder.windowSize(instance.windowSize());
                }
                yield builder;
            }
            case 1 -> new InferenceRescorerBuilder(
                instance.getModelId(),
                instance.getInferenceConfigUpdate(),
                (Supplier<ModelLoadingService>) null).windowSize(
                randomValueOtherThan(instance.windowSize(), () -> randomIntBetween(1, 10000))
            );
            case 2 -> {
                InferenceRescorerBuilder builder = new InferenceRescorerBuilder(
                    instance.getModelId(),
                    LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate(),
                    (Supplier<ModelLoadingService>) null);
                if (instance.windowSize() != null) {
                    builder.windowSize(instance.windowSize());
                }
                yield builder;
            }
            default -> throw new AssertionError("Unexpected random test case");
        };
    }

    @Override
    protected InferenceRescorerBuilder mutateInstanceForVersion(InferenceRescorerBuilder instance, TransportVersion version) {
        return instance;
    }

    public void testIncorrectInferenceConfigType() {
        InferenceRescorerBuilder.Builder builder = new InferenceRescorerBuilder.Builder();
        expectThrows(
            IllegalArgumentException.class,
            () -> builder.setInferenceConfigUpdate(ClassificationConfigUpdateTests.randomClassificationConfigUpdate())
        );
        //Should not throw
        builder.setInferenceConfigUpdate(LearnToRankConfigUpdateTests.randomLearnToRankConfigUpdate());
    }
}
