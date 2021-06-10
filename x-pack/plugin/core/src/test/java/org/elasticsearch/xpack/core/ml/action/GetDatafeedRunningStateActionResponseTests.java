/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction.Response;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetDatafeedRunningStateActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    static Response.RunningState randomRunningState() {
        return new Response.RunningState(randomBoolean(), randomBoolean());
    }

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        return new Response(Stream.generate(() -> randomAlphaOfLength(10))
            .limit(listSize)
            .collect(Collectors.toMap(Function.identity(), _unused -> randomRunningState())));
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

}
