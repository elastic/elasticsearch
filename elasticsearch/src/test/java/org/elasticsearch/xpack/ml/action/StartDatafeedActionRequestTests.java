/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class StartDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<StartDatafeedAction.Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLength(10), randomNonNegativeLong());
        if (randomBoolean()) {
            request.setEndTime(randomNonNegativeLong());
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    public void testValidate() {
        Job job1 = DatafeedJobRunnerTests.createDatafeedJob().build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StartDatafeedAction.validate("some-datafeed", mlMetadata1,
                        new PersistentTasksInProgress(0L, Collections.emptyMap())));
        assertThat(e.getMessage(), equalTo("No datafeed with id [some-datafeed] exists"));

        PersistentTaskInProgress<OpenJobAction.Request> task =
                new PersistentTaskInProgress<>(0L, OpenJobAction.NAME, new OpenJobAction.Request("foo"), null);
        PersistentTasksInProgress tasks = new PersistentTasksInProgress(0L, Collections.singletonMap(0L, task));
        DatafeedConfig datafeedConfig1 = DatafeedJobRunnerTests.createDatafeedConfig("foo-datafeed", "foo").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1)
                .build();
        e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks));
        assertThat(e.getMessage(), equalTo("cannot start datafeed, expected job state [opened], but got [closed]"));
    }

}
