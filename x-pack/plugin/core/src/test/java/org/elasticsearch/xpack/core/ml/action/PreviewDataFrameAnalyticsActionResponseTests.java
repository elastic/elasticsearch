/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction.Response;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PreviewDataFrameAnalyticsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(
            Stream.generate(() -> randomHashMap("foo", "bar", "baz")).limit(randomIntBetween(1, 10)).collect(Collectors.toList())
        );
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    private static Map<String, Object> randomHashMap(String... keys) {
        return Arrays.stream(keys).collect(Collectors.toMap(Function.identity(), k -> randomAlphaOfLength(10)));

    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
