/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction.Request;

import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigUpdateTests.randomDataFrameTransformConfigUpdate;

public class UpdateDataFrameTransformActionRequestTests extends AbstractWireSerializingDataFrameTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(randomDataFrameTransformConfigUpdate(), randomAlphaOfLength(10), randomBoolean());
    }

}
