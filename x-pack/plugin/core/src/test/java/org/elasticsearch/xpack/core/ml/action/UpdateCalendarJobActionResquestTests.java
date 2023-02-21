/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class UpdateCalendarJobActionResquestTests extends AbstractWireSerializingTestCase<UpdateCalendarJobAction.Request> {

    @Override
    protected UpdateCalendarJobAction.Request createTestInstance() {
        return new UpdateCalendarJobAction.Request(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected UpdateCalendarJobAction.Request mutateInstance(UpdateCalendarJobAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<UpdateCalendarJobAction.Request> instanceReader() {
        return UpdateCalendarJobAction.Request::new;
    }
}
