/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class StartDataFrameTransformTaskActionRequestTests extends
        AbstractWireSerializingTestCase<StartDataFrameTransformTaskAction.Request> {
    @Override
    protected StartDataFrameTransformTaskAction.Request createTestInstance() {
        return new StartDataFrameTransformTaskAction.Request(randomAlphaOfLength(4), randomBoolean());
    }

    @Override
    protected Writeable.Reader<StartDataFrameTransformTaskAction.Request> instanceReader() {
        return StartDataFrameTransformTaskAction.Request::new;
    }
}
