/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.action.InferModelActionRequestTests;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.inference.InferenceNamedWriteablesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CoordinatedInferenceActionRequestTests extends AbstractWireSerializingTestCase<CoordinatedInferenceAction.Request> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testWrap() {
        SearchPhaseExecutionException cause = new SearchPhaseExecutionException("foo", "bar", new ShardSearchFailure[] {});
        ElasticsearchException wrapper = new ElasticsearchException("foo", cause);
        assertEquals(wrapper, ExceptionsHelper.unwrapCause(wrapper));
    }

    @Override
    protected Writeable.Reader<CoordinatedInferenceAction.Request> instanceReader() {
        return CoordinatedInferenceAction.Request::new;
    }

    @Override
    protected CoordinatedInferenceAction.Request createTestInstance() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                var inferenceConfig = randomBoolean() ? null : InferModelActionRequestTests.randomInferenceConfigUpdate();
                var previouslyLicensed = randomBoolean() ? null : randomBoolean();
                var inferenceTimeout = randomBoolean() ? null : TimeValue.parseTimeValue(randomTimeValue(), null, "timeout");
                var highPriority = randomBoolean() ? null : randomBoolean();

                var request = CoordinatedInferenceAction.Request.forTextInput(
                    randomAlphaOfLength(6),
                    List.of(randomAlphaOfLength(6)),
                    inferenceConfig,
                    previouslyLicensed,
                    inferenceTimeout
                );
                request.setHighPriority(highPriority);
                yield request;
            }
            case 1 -> {
                var inferenceConfig = randomBoolean() ? null : InferModelActionRequestTests.randomInferenceConfigUpdate();
                var previouslyLicensed = randomBoolean() ? null : randomBoolean();
                var inferenceTimeout = randomBoolean() ? null : TimeValue.parseTimeValue(randomTimeValue(), null, "timeout");
                var highPriority = randomBoolean() ? null : randomBoolean();

                var request = CoordinatedInferenceAction.Request.forMapInput(
                    randomAlphaOfLength(6),
                    Stream.generate(CoordinatedInferenceActionRequestTests::randomMap).limit(randomInt(5)).collect(Collectors.toList()),
                    inferenceConfig,
                    previouslyLicensed,
                    inferenceTimeout
                );
                request.setHighPriority(highPriority);
                yield request;
            }
            case 2 -> {
                var taskSettings = randomBoolean() ? null : randomMap();

                yield CoordinatedInferenceAction.Request.forInferenceService(
                    randomAlphaOfLength(6),
                    List.of(randomAlphaOfLength(6)),
                    taskSettings
                );
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    private static Map<String, Object> randomMap() {
        return Stream.generate(() -> randomAlphaOfLength(10))
            .limit(randomInt(10))
            .collect(Collectors.toMap(Function.identity(), (v) -> randomAlphaOfLength(10)));
    }

    @Override
    protected CoordinatedInferenceAction.Request mutateInstance(CoordinatedInferenceAction.Request instance) throws IOException {
        return null;
    }
}
