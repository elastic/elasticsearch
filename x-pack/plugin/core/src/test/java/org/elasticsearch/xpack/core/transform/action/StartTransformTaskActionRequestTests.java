/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StartTransformTaskActionRequestTests extends
        AbstractWireSerializingTestCase<StartTransformTaskAction.Request> {
    @Override
    protected StartTransformTaskAction.Request createTestInstance() {
        return new StartTransformTaskAction.Request(randomAlphaOfLength(4), randomBoolean());
    }

    @Override
    protected Writeable.Reader<StartTransformTaskAction.Request> instanceReader() {
        return StartTransformTaskAction.Request::new;
    }
}
