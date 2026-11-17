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

public class SaveEventActionResponseTests extends AbstractWireSerializingTestCase<SaveEventAction.Response> {

    @Override
    protected Writeable.Reader<SaveEventAction.Response> instanceReader() {
        return SaveEventAction.Response::new;
    }

    @Override
    protected SaveEventAction.Response createTestInstance() {
        return new SaveEventAction.Response(SessionEventTestUtils.randomSessionEvent());
    }

    @Override
    protected SaveEventAction.Response mutateInstance(SaveEventAction.Response instance) {
        return new SaveEventAction.Response(randomValueOtherThan(instance.event(), SessionEventTestUtils::randomSessionEvent));
    }
}
