/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsInternalAction;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsInternalAction.GET_INFERENCE_FIELDS_ACTION_AS_INDICES_ACTION_TV;

public class GetInferenceFieldsInternalActionRequestTests extends AbstractBWCWireSerializationTestCase<
    GetInferenceFieldsInternalAction.Request> {
    @Override
    protected Writeable.Reader<GetInferenceFieldsInternalAction.Request> instanceReader() {
        return GetInferenceFieldsInternalAction.Request::new;
    }

    @Override
    protected GetInferenceFieldsInternalAction.Request createTestInstance() {
        return new GetInferenceFieldsInternalAction.Request(
            randomIndices(),
            randomFields(),
            randomBoolean(),
            randomBoolean(),
            randomQuery(),
            randomIndicesOptions()
        );
    }

    @Override
    protected GetInferenceFieldsInternalAction.Request mutateInstance(GetInferenceFieldsInternalAction.Request instance)
        throws IOException {
        return switch (between(0, 5)) {
            case 0 -> new GetInferenceFieldsInternalAction.Request(
                randomArrayOtherThan(instance.indices(), GetInferenceFieldsInternalActionRequestTests::randomIndices),
                instance.fields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.query(),
                instance.indicesOptions()
            );
            case 1 -> new GetInferenceFieldsInternalAction.Request(
                instance.indices(),
                randomValueOtherThan(instance.fields(), GetInferenceFieldsInternalActionRequestTests::randomFields),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.query(),
                instance.indicesOptions()
            );
            case 2 -> new GetInferenceFieldsInternalAction.Request(
                instance.indices(),
                instance.fields(),
                randomValueOtherThan(instance.resolveWildcards(), ESTestCase::randomBoolean),
                instance.useDefaultFields(),
                instance.query(),
                instance.indicesOptions()
            );
            case 3 -> new GetInferenceFieldsInternalAction.Request(
                instance.indices(),
                instance.fields(),
                instance.resolveWildcards(),
                randomValueOtherThan(instance.useDefaultFields(), ESTestCase::randomBoolean),
                instance.query(),
                instance.indicesOptions()
            );
            case 4 -> new GetInferenceFieldsInternalAction.Request(
                instance.indices(),
                instance.fields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                randomValueOtherThan(instance.query(), GetInferenceFieldsInternalActionRequestTests::randomQuery),
                instance.indicesOptions()
            );
            case 5 -> new GetInferenceFieldsInternalAction.Request(
                instance.indices(),
                instance.fields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.query(),
                randomValueOtherThan(instance.indicesOptions(), () -> {
                    IndicesOptions newOptions = randomIndicesOptions();
                    while (instance.indicesOptions() == IndicesOptions.DEFAULT && newOptions == null) {
                        newOptions = randomIndicesOptions();
                    }
                    return newOptions;
                })
            );
            default -> throw new AssertionError("Invalid value");
        };
    }

    @Override
    protected Collection<TransportVersion> bwcVersions() {
        TransportVersion minVersion = TransportVersion.max(
            TransportVersion.minimumCompatible(),
            GET_INFERENCE_FIELDS_ACTION_AS_INDICES_ACTION_TV
        );
        return TransportVersionUtils.allReleasedVersions().tailSet(minVersion, true);
    }

    @Override
    protected GetInferenceFieldsInternalAction.Request mutateInstanceForVersion(
        GetInferenceFieldsInternalAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static String[] randomIndices() {
        return randomArray(0, 5, String[]::new, ESTestCase::randomIdentifier);
    }

    private static Map<String, Float> randomFields() {
        return randomMap(0, 5, () -> Tuple.tuple(randomIdentifier(), randomFloat()));
    }

    private static String randomQuery() {
        return randomBoolean() ? randomAlphaOfLengthBetween(5, 10) : null;
    }

    private static IndicesOptions randomIndicesOptions() {
        // This isn't an exhaustive list of possible indices options, but there are enough for effective serialization tests.
        // Omit IndicesOptions.strictExpandOpen() because it is equal to IndicesOptions#DEFAULT, which we use in the null case.
        return switch (between(0, 8)) {
            case 0 -> null;
            case 1 -> IndicesOptions.strictExpandOpenFailureNoSelectors();
            case 2 -> IndicesOptions.strictExpandOpenAndForbidClosed();
            case 3 -> IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled();
            case 4 -> IndicesOptions.strictExpand();
            case 5 -> IndicesOptions.strictExpandHidden();
            case 6 -> IndicesOptions.strictExpandHiddenNoSelectors();
            case 7 -> IndicesOptions.strictExpandHiddenFailureNoSelectors();
            case 8 -> IndicesOptions.strictNoExpandForbidClosed();
            default -> throw new AssertionError("Invalid value");
        };
    }
}
