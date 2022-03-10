/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class StartTrainedModelDeploymentTaskParamsTests extends AbstractSerializingTestCase<TaskParams> {

    @Override
    protected TaskParams doParseInstance(XContentParser parser) throws IOException {
        return TaskParams.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TaskParams> instanceReader() {
        return TaskParams::new;
    }

    @Override
    protected TaskParams createTestInstance() {
        return createRandom();
    }

    public static StartTrainedModelDeploymentAction.TaskParams createRandom() {
        boolean canModelThreadsBeGreaterThanOne = randomBoolean();
        boolean canInferenceThreadsBeGreaterThanOne = canModelThreadsBeGreaterThanOne == false;

        return new TaskParams(
            randomAlphaOfLength(10),
            randomNonNegativeLong(),
            canInferenceThreadsBeGreaterThanOne ? randomIntBetween(1, 8) : 1,
            canModelThreadsBeGreaterThanOne ? randomIntBetween(1, 8) : 1,
            randomIntBetween(1, 10000)
        );
    }

    public void testCtor_GivenBothModelAndInferenceThreadsGreaterThanOne_AndMoreModelThreads() {
        TaskParams taskParams = new TaskParams(randomAlphaOfLength(10), randomNonNegativeLong(), 4, 5, randomIntBetween(1, 10000));
        assertThat(taskParams.getModelThreads(), equalTo(9));
        assertThat(taskParams.getInferenceThreads(), equalTo(1));
    }

    public void testCtor_GivenBothModelAndInferenceThreadsGreaterThanOne_AndMoreInferenceThreads() {
        TaskParams taskParams = new TaskParams(randomAlphaOfLength(10), randomNonNegativeLong(), 3, 2, randomIntBetween(1, 10000));
        assertThat(taskParams.getModelThreads(), equalTo(1));
        assertThat(taskParams.getInferenceThreads(), equalTo(5));
    }

    public void testCtor_GivenBothModelAndInferenceThreadsGreaterThanOne_AndTie() {
        TaskParams taskParams = new TaskParams(randomAlphaOfLength(10), randomNonNegativeLong(), 4, 4, randomIntBetween(1, 10000));
        assertThat(taskParams.getModelThreads(), equalTo(8));
        assertThat(taskParams.getInferenceThreads(), equalTo(1));
    }
}
