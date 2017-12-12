/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class GetCalendarsActionRequestTests extends AbstractStreamableTestCase<GetCalendarsAction.Request> {


    @Override
    protected GetCalendarsAction.Request createTestInstance() {
        GetCalendarsAction.Request request = new GetCalendarsAction.Request();
        request.setCalendarId(randomAlphaOfLengthBetween(1, 20));
        return request;
    }

    @Override
    protected GetCalendarsAction.Request createBlankInstance() {
        return new GetCalendarsAction.Request();
    }

}
