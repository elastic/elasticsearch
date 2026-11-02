/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.events.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.contextengine.events.SessionEventTestUtils;

public class GetSessionEventsActionResponseTests extends AbstractWireSerializingTestCase<GetSessionEventsAction.Response> {

    @Override
    protected Writeable.Reader<GetSessionEventsAction.Response> instanceReader() {
        return GetSessionEventsAction.Response::new;
    }

    @Override
    protected GetSessionEventsAction.Response createTestInstance() {
        return new GetSessionEventsAction.Response(randomList(0, 5, SessionEventTestUtils::randomSessionEvent));
    }

    @Override
    protected GetSessionEventsAction.Response mutateInstance(GetSessionEventsAction.Response instance) {
        return new GetSessionEventsAction.Response(
            randomValueOtherThan(instance.events(), () -> randomList(0, 5, SessionEventTestUtils::randomSessionEvent))
        );
    }
}
