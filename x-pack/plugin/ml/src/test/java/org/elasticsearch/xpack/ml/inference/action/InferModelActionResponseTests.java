/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.inference.action.InferModelAction.Response;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferModelActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    @SuppressWarnings("unchecked")
    protected Response createTestInstance() {
        Supplier<Object> resultSupplier = randomFrom(() -> randomAlphaOfLength(10),
            ESTestCase::randomDouble,
            () -> Stream.generate(() -> randomAlphaOfLength(10))
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.toMap(Function.identity(), v -> randomDouble())));
        return new Response(Stream.generate(resultSupplier).limit(randomIntBetween(0, 10)).collect(Collectors.toList()));
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
