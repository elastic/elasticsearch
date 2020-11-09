/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction.Request;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GetJobStatsActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomBoolean() ? Metadata.ALL : randomAlphaOfLengthBetween(1, 20));
        request.setAllowNoMatch(randomBoolean());
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testMatch_GivenAll_FailsForNonJobTasks() {
        Task nonJobTask = mock(Task.class);

        assertThat(new Request("_all").match(nonJobTask), is(false));
    }
}
