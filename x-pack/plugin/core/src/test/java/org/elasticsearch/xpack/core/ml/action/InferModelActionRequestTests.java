/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.InferModelAction.Request;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InferModelActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLength(10),
            randomLongBetween(1, 100),
            randomBoolean() ? null : Stream.generate(()-> randomAlphaOfLength(10))
                .limit(randomInt(10))
                .collect(Collectors.toMap(Function.identity(), (v) -> randomAlphaOfLength(10))),
            randomBoolean() ? null : randomIntBetween(-1, 100));
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

}
