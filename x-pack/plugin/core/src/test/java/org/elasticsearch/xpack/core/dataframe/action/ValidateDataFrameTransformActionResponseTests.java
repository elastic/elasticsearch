/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.action.ValidateDataFrameTransformAction.Response;

import java.util.Arrays;


public class ValidateDataFrameTransformActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new ValidateDataFrameTransformAction.Response(Arrays.asList(generateRandomStringArray(10, 12, false)));
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

}
