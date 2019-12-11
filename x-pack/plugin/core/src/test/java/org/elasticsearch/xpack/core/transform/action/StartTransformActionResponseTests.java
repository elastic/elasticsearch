/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction.Response;

public class StartTransformActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(randomBoolean());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

}
