/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction.Response;

import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

public class ValidateTransformActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(randomDestIndexMappings());
    }

    private Map<String, String> randomDestIndexMappings() {
        return randomList(1, 20, () -> randomAlphaOfLengthBetween(1, 20)).stream()
            .distinct()
            .collect(toMap(Function.identity(), fieldName -> randomAlphaOfLengthBetween(1, 20)));
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }
}
