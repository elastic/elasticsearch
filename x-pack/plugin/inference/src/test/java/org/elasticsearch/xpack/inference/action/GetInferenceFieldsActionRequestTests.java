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
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction.GET_INFERENCE_FIELDS_ACTION_TV;

public class GetInferenceFieldsActionRequestTests extends AbstractBWCWireSerializationTestCase<GetInferenceFieldsAction.Request> {
    @Override
    protected Writeable.Reader<GetInferenceFieldsAction.Request> instanceReader() {
        return GetInferenceFieldsAction.Request::new;
    }

    @Override
    protected GetInferenceFieldsAction.Request createTestInstance() {
        return new GetInferenceFieldsAction.Request(
            randomIndentifierSet(),
            randomFields(),
            randomBoolean(),
            randomBoolean(),
            randomQuery(),
            randomIndicesOptions()
        );
    }

    @Override
    protected GetInferenceFieldsAction.Request mutateInstance(GetInferenceFieldsAction.Request instance) throws IOException {
        return switch (between(0, 5)) {
            case 0 -> new GetInferenceFieldsAction.Request(
                randomValueOtherThan(instance.getIndices(), GetInferenceFieldsActionRequestTests::randomIndentifierSet),
                instance.getFields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.getQuery(),
                instance.getIndicesOptions()
            );
            case 1 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                randomValueOtherThan(instance.getFields(), GetInferenceFieldsActionRequestTests::randomFields),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.getQuery(),
                instance.getIndicesOptions()
            );
            case 2 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                randomValueOtherThan(instance.resolveWildcards(), ESTestCase::randomBoolean),
                instance.useDefaultFields(),
                instance.getQuery(),
                instance.getIndicesOptions()
            );
            case 3 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                instance.resolveWildcards(),
                randomValueOtherThan(instance.useDefaultFields(), ESTestCase::randomBoolean),
                instance.getQuery(),
                instance.getIndicesOptions()
            );
            case 4 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                randomValueOtherThan(instance.getQuery(), GetInferenceFieldsActionRequestTests::randomQuery),
                instance.getIndicesOptions()
            );
            case 5 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.getQuery(),
                randomValueOtherThan(instance.getIndicesOptions(), () -> {
                    IndicesOptions newOptions = randomIndicesOptions();
                    while (instance.getIndicesOptions() == IndicesOptions.DEFAULT && newOptions == null) {
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
        TransportVersion minVersion = TransportVersion.max(TransportVersion.minimumCompatible(), GET_INFERENCE_FIELDS_ACTION_TV);
        return TransportVersionUtils.allReleasedVersions().tailSet(minVersion, true);
    }

    @Override
    protected GetInferenceFieldsAction.Request mutateInstanceForVersion(
        GetInferenceFieldsAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static Set<String> randomIndentifierSet() {
        return randomSet(0, 5, ESTestCase::randomIdentifier);
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
