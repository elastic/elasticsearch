/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

public class CoordinatedInferenceActionRequestTests extends AbstractBWCWireSerializationTestCase<CoordinatedInferenceAction.Request> {
    public void testSerializesPrefixType_WhenTransportVersionIs_InputTypeAdded() throws IOException {
        var instance = createTestInstance();
        instance.setPrefixType(TrainedModelPrefixStrings.PrefixType.INGEST);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), TransportVersions.V_8_13_0);
        assertOnBWCObject(copy, instance, TransportVersions.V_8_13_0);
        assertThat(copy.getPrefixType(), is(TrainedModelPrefixStrings.PrefixType.INGEST));
    }

    public void testSerializesPrefixType_DoesNotSerialize_WhenTransportVersion_IsPriorToInputTypeAdded() throws IOException {
        var instance = createTestInstance();
        instance.setPrefixType(TrainedModelPrefixStrings.PrefixType.INGEST);
        var copy = copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), TransportVersions.V_8_12_1);

        assertNotSame(copy, instance);
        assertNotEquals(copy, instance);
        assertNotEquals(copy.hashCode(), instance.hashCode());
        assertThat(copy.getPrefixType(), is(TrainedModelPrefixStrings.PrefixType.NONE));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<CoordinatedInferenceAction.Request> instanceReader() {
        return CoordinatedInferenceAction.Request::new;
    }

    @Override
    protected CoordinatedInferenceAction.Request createTestInstance() {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> {
                var inferenceConfig = randomBoolean() ? null : InferModelActionRequestTests.randomInferenceConfigUpdate();
                var previouslyLicensed = randomBoolean() ? null : randomBoolean();
                var inferenceTimeout = randomBoolean() ? null : randomTimeValue();
                var highPriority = randomBoolean();

                var request = CoordinatedInferenceAction.Request.forTextInput(
                    randomAlphaOfLength(6),
                    List.of(randomAlphaOfLength(6)),
                    inferenceConfig,
                    previouslyLicensed,
                    inferenceTimeout
                );
                request.setHighPriority(highPriority);
                request.setPrefixType(randomFrom(TrainedModelPrefixStrings.PrefixType.values()));
                yield request;
            }
            case 1 -> {
                var inferenceConfig = randomBoolean() ? null : InferModelActionRequestTests.randomInferenceConfigUpdate();
                var previouslyLicensed = randomBoolean() ? null : randomBoolean();
                var inferenceTimeout = randomBoolean() ? null : randomTimeValue();
                var highPriority = randomBoolean();
                var modelType = randomFrom(CoordinatedInferenceAction.Request.RequestModelType.values());

                var request = CoordinatedInferenceAction.Request.forMapInput(
                    randomAlphaOfLength(6),
                    Stream.generate(CoordinatedInferenceActionRequestTests::randomMap).limit(randomInt(5)).collect(Collectors.toList()),
                    inferenceConfig,
                    previouslyLicensed,
                    inferenceTimeout,
                    modelType
                );
                request.setHighPriority(highPriority);
                request.setPrefixType(randomFrom(TrainedModelPrefixStrings.PrefixType.values()));
                yield request;
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

    @Override
    protected CoordinatedInferenceAction.Request mutateInstanceForVersion(
        CoordinatedInferenceAction.Request instance,
        TransportVersion version
    ) {
        if (version.before(TransportVersions.V_8_13_0)) {
            instance.setPrefixType(TrainedModelPrefixStrings.PrefixType.NONE);
        }

        var newInstance = new CoordinatedInferenceAction.Request(
            instance.getModelId(),
            instance.getInputs(),
            instance.getTaskSettings(),
            instance.getObjectsToInfer(),
            InferModelActionRequestTests.mutateInferenceConfigUpdate(instance.getInferenceConfigUpdate(), version),
            instance.getPreviouslyLicensed(),
            instance.getInferenceTimeout(),
            instance.getHighPriority(),
            instance.getRequestModelType()
        );
        newInstance.setPrefixType(instance.getPrefixType());
        return newInstance;
    }
}
