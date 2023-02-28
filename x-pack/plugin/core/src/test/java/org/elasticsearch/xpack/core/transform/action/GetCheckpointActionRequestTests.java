/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class GetCheckpointActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(
            randomBoolean() ? null : generateRandomStringArray(10, 10, false, false),
            IndicesOptions.fromParameters(
                randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                SearchRequest.DEFAULT_INDICES_OPTIONS
            )
        );
    }

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        List<String> indices = instance.indices() != null ? new ArrayList<>(Arrays.asList(instance.indices())) : new ArrayList<>();
        IndicesOptions indicesOptions = instance.indicesOptions();

        switch (between(0, 1)) {
            case 0:
                indices.add(randomAlphaOfLengthBetween(1, 20));
                break;
            case 1:
                indicesOptions = IndicesOptions.fromParameters(
                    randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                    Boolean.toString(instance.indicesOptions().ignoreUnavailable() == false),
                    Boolean.toString(instance.indicesOptions().allowNoIndices() == false),
                    Boolean.toString(instance.indicesOptions().ignoreThrottled() == false),
                    SearchRequest.DEFAULT_INDICES_OPTIONS
                );
                break;
            default:
                throw new AssertionError("Illegal randomization branch");
        }

        return new Request(indices.toArray(new String[0]), indicesOptions);
    }
}
