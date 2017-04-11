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
import org.elasticsearch.xpack.ml.MlMetadata.Builder;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTaskRequest;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedConfig;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedJob;
import static org.hamcrest.Matchers.equalTo;

public class StopDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setStopTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        request.setForce(randomBoolean());
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
        PersistentTask<?> task = new PersistentTask<PersistentTaskRequest>("1L", StartDatafeedAction.NAME,
                new StartDatafeedAction.Request("foo", 0L), 1L, new PersistentTasksCustomMetaData.Assignment("node_id", ""));
        task = new PersistentTask<>(task, DatafeedState.STARTED);
        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap("1L", task));

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
            PersistentTask<?> task = new PersistentTask<PersistentTaskRequest>("1L", StartDatafeedAction.NAME,
                    new StartDatafeedAction.Request("foo2", 0L), 1L, new PersistentTasksCustomMetaData.Assignment("node_id", ""));
            tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap("1L", task));
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

    public void testResolveAll() {
        Map<String, PersistentTask<?>> taskMap = new HashMap<>();
        Builder mlMetadataBuilder = new MlMetadata.Builder();

        PersistentTask<?> task = new PersistentTask<PersistentTaskRequest>("1L", StartDatafeedAction.NAME,
                new StartDatafeedAction.Request("datafeed_1", 0L), 1L, new PersistentTasksCustomMetaData.Assignment("node_id", ""));
        task = new PersistentTask<>(task, DatafeedState.STARTED);
        taskMap.put("1L", task);
        Job job = BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed_1", "job_id_1").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        task = new PersistentTask<PersistentTaskRequest>("2L", StartDatafeedAction.NAME,
                new StartDatafeedAction.Request("datafeed_2", 0L), 2L, new PersistentTasksCustomMetaData.Assignment("node_id", ""));
        task = new PersistentTask<>(task, DatafeedState.STOPPED);
        taskMap.put("2L", task);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_2").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_2", "job_id_2").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        task = new PersistentTask<PersistentTaskRequest>("3L", StartDatafeedAction.NAME,
                new StartDatafeedAction.Request("datafeed_3", 0L), 3L, new PersistentTasksCustomMetaData.Assignment("node_id", ""));
        task = new PersistentTask<>(task, DatafeedState.STARTED);
        taskMap.put("3L", task);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_3").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_3", "job_id_3").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(3L, taskMap);
        MlMetadata mlMetadata = mlMetadataBuilder.build();

        assertEquals(Arrays.asList("datafeed_1", "datafeed_3"), StopDatafeedAction.resolve("_all", mlMetadata, tasks));
    }

}
