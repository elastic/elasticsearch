/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetMlAutoscalingStats.Response;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ml.autoscaling.AutoscalingResourcesTests.randomAutoscalingResources;

public class GetMlAutoscalingStatsResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(randomAutoscalingResources());
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        return null; // TODO
    }
}
