/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction.Response;

public class ScheduleNowTransformActionResponseTests extends AbstractWireSerializingTransformTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(randomBoolean());
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response mutateInstance(Response instance) {
        boolean acknowledged = instance.isAcknowledged();
        return new Response(acknowledged == false);
    }

    public void testResponseTrue() {
        assertTrue(Response.TRUE.isAcknowledged());
    }
}
