/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeprecationInfoActionRequestTests extends AbstractWireSerializingTestCase<DeprecationInfoAction.Request> {

    @Override
    protected DeprecationInfoAction.Request createTestInstance() {
        return new DeprecationInfoAction.Request(randomAlphaOfLength(10));
    }

    @Override
    protected DeprecationInfoAction.Request mutateInstance(DeprecationInfoAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<DeprecationInfoAction.Request> instanceReader() {
        return DeprecationInfoAction.Request::new;
    }
}
