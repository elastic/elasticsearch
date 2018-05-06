/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class UpdateCalendarJobActionResquestTests extends AbstractStreamableTestCase<UpdateCalendarJobAction.Request> {

    @Override
    protected UpdateCalendarJobAction.Request createTestInstance() {
        return new UpdateCalendarJobAction.Request(randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Override
    protected UpdateCalendarJobAction.Request  createBlankInstance() {
        return new UpdateCalendarJobAction.Request();
    }
}
