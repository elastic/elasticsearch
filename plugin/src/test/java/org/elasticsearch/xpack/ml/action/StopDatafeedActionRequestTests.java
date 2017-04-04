/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.persistent.PersistentTaskRequest;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.util.Collections;
import java.util.Date;

import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedConfig;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedJob;
import static org.hamcrest.Matchers.equalTo;

public class StopDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
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
        PersistentTask<?> task = new PersistentTask<PersistentTaskRequest>(1L, StartDatafeedAction.NAME,
                new StartDatafeedAction.Request("foo", 0L), new PersistentTasksCustomMetaData.Assignment("node_id", ""));
        task = new PersistentTask<>(task, DatafeedState.STARTED);
        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap(1L, task));

        Job job = createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder().putJob(job, false).build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StopDatafeedAction.validateAndReturnDatafeedTask("foo", mlMetadata1, tasks));
        assertThat(e.getMessage(), equalTo("No datafeed with id [foo] exists"));

        DatafeedConfig datafeedConfig = createDatafeedConfig("foo", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder().putJob(job, false)
                .putDatafeed(datafeedConfig)
                .build();
        StopDatafeedAction.validateAndReturnDatafeedTask("foo", mlMetadata2, tasks);
    }

    public void testValidate_alreadyStopped() {
        PersistentTasksCustomMetaData tasks;
        if (randomBoolean()) {
            PersistentTask<?> task = new PersistentTask<PersistentTaskRequest>(1L, StartDatafeedAction.NAME,
                    new StartDatafeedAction.Request("foo2", 0L), new PersistentTasksCustomMetaData.Assignment("node_id", ""));
            tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap(1L, task));
        } else {
            tasks = randomBoolean() ? null : new PersistentTasksCustomMetaData(0L, Collections.emptyMap());
        }

        Job job = createDatafeedJob().build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("foo", "job_id").build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job, false)
                .putDatafeed(datafeedConfig)
                .build();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StopDatafeedAction.validateAndReturnDatafeedTask("foo", mlMetadata1, tasks));
        assertThat(e.getMessage(), equalTo("Cannot stop datafeed [foo] because it has already been stopped"));
    }

}
