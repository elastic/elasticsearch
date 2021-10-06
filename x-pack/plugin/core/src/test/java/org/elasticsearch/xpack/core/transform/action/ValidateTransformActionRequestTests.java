/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction.Request;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomTransformConfig;

public class ValidateTransformActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomTransformConfig(), randomBoolean());
    }

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }
}
