/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DeprecationInfoActionRequestTests extends AbstractWireSerializingTestCase<DeprecationInfoAction.Request> {

    @Override
    protected DeprecationInfoAction.Request createTestInstance() {
        return new DeprecationInfoAction.Request(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<DeprecationInfoAction.Request> instanceReader() {
        return DeprecationInfoAction.Request::new;
    }
}
