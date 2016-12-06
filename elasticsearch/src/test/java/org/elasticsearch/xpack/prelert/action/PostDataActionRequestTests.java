/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

public class PostDataActionRequestTests extends AbstractStreamableTestCase<JobDataAction.Request> {
    @Override
    protected JobDataAction.Request createTestInstance() {
        JobDataAction.Request request = new JobDataAction.Request(randomAsciiOfLengthBetween(1, 20));
        request.setIgnoreDowntime(randomBoolean());
        if (randomBoolean()) {
            request.setResetStart(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setResetEnd(randomAsciiOfLengthBetween(1, 20));
        }
        return request;
    }

    @Override
    protected JobDataAction.Request createBlankInstance() {
        return new JobDataAction.Request();
    }
}
