/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction.Request;

public class GetJobsActionRequestTests extends AbstractStreamableTestCase<GetJobsAction.Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomBoolean() ? MetaData.ALL : randomAlphaOfLengthBetween(1, 20));
        request.setAllowNoJobs(randomBoolean());
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
