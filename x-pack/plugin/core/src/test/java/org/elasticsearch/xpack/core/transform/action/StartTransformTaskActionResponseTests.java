/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StartTransformTaskActionResponseTests extends
        AbstractWireSerializingTestCase<StartTransformTaskAction.Response> {
    @Override
    protected StartTransformTaskAction.Response createTestInstance() {
        return new StartTransformTaskAction.Response(randomBoolean());
    }

    @Override
    protected Writeable.Reader<StartTransformTaskAction.Response> instanceReader() {
        return StartTransformTaskAction.Response::new;
    }
}
