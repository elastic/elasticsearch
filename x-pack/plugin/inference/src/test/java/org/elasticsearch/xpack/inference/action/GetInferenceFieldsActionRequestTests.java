/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;

import java.io.IOException;
import java.util.Set;

public class GetInferenceFieldsActionRequestTests extends AbstractWireSerializingTestCase<GetInferenceFieldsAction.Request> {
    @Override
    protected Writeable.Reader<GetInferenceFieldsAction.Request> instanceReader() {
        return GetInferenceFieldsAction.Request::new;
    }

    @Override
    protected GetInferenceFieldsAction.Request createTestInstance() {
        return new GetInferenceFieldsAction.Request(
            randomIndentifierSet(),
            randomIndentifierSet(),
            randomBoolean(),
            randomBoolean(),
            randomQuery()
        );
    }

    @Override
    protected GetInferenceFieldsAction.Request mutateInstance(GetInferenceFieldsAction.Request instance) throws IOException {
        return switch (between(0, 4)) {
            case 0 -> new GetInferenceFieldsAction.Request(
                randomValueOtherThan(instance.getIndices(), GetInferenceFieldsActionRequestTests::randomIndentifierSet),
                instance.getFields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.getQuery()
            );
            case 1 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                randomValueOtherThan(instance.getFields(), GetInferenceFieldsActionRequestTests::randomIndentifierSet),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                instance.getQuery()
            );
            case 2 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                randomValueOtherThan(instance.resolveWildcards(), ESTestCase::randomBoolean),
                instance.useDefaultFields(),
                instance.getQuery()
            );
            case 3 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                instance.resolveWildcards(),
                randomValueOtherThan(instance.useDefaultFields(), ESTestCase::randomBoolean),
                instance.getQuery()
            );
            case 4 -> new GetInferenceFieldsAction.Request(
                instance.getIndices(),
                instance.getFields(),
                instance.resolveWildcards(),
                instance.useDefaultFields(),
                randomValueOtherThan(instance.getQuery(), GetInferenceFieldsActionRequestTests::randomQuery)
            );
            default -> throw new AssertionError("Invalid value");
        };
    }

    private static Set<String> randomIndentifierSet() {
        return randomSet(1, 5, ESTestCase::randomIdentifier);
    }

    private static String randomQuery() {
        return randomBoolean() ? randomAlphaOfLength(10) : null;
    }
}
