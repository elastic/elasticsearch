/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Collections;
import java.util.Date;

import static org.elasticsearch.persistent.PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT;
import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TransportStartDatafeedActionTests extends ESTestCase {

    public void testValidate_GivenDatafeedIsMissing() {
        Job job = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> TransportStartDatafeedAction.validate("some-datafeed", mlMetadata, null));
        assertThat(e.getMessage(), equalTo("No datafeed with id [some-datafeed] exists"));
    }

    public void testValidate_jobClosed() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTasksCustomMetaData tasks = PersistentTasksCustomMetaData.builder().build();
        DatafeedConfig datafeedConfig1 = DatafeedManagerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1, Collections.emptyMap())
                .build();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> TransportStartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks));
        assertThat(e.getMessage(), equalTo("cannot start datafeed [foo-datafeed] because job [job_id] is closed"));
    }

    public void testValidate_jobOpening() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedManagerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1, Collections.emptyMap())
                .build();

        TransportStartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks);
    }

    public void testValidate_jobOpened() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedManagerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1, Collections.emptyMap())
                .build();

        TransportStartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks);
    }

    public void testDeprecationsLogged() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        DatafeedConfig.Builder datafeedConfig = DatafeedManagerTests.createDatafeedConfig("start-data-feed-test", job1.getId());
        DatafeedConfig config = spy(datafeedConfig.build());
        doReturn(Collections.singletonList("Deprecated Agg")).when(config).getAggDeprecations();
        doReturn(Collections.singletonList("Deprecated Query")).when(config).getQueryDeprecations();

        Auditor auditor = mock(Auditor.class);

        TransportStartDatafeedAction.auditDeprecations(config, job1, auditor);

        verify(auditor).warning(job1.getId(),
            "datafeed [start-data-feed-test] configuration has deprecations. [Deprecated Agg, Deprecated Query]");
    }

    public void testNoDeprecationsLogged() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        DatafeedConfig.Builder datafeedConfig = DatafeedManagerTests.createDatafeedConfig("start-data-feed-test", job1.getId());
        DatafeedConfig config = spy(datafeedConfig.build());
        doReturn(Collections.emptyList()).when(config).getAggDeprecations();
        doReturn(Collections.emptyList()).when(config).getQueryDeprecations();

        Auditor auditor = mock(Auditor.class);

        TransportStartDatafeedAction.auditDeprecations(config, job1, auditor);

        verify(auditor, never()).warning(any(), any());
    }

    public static TransportStartDatafeedAction.DatafeedTask createDatafeedTask(long id, String type, String action,
                                                                               TaskId parentTaskId,
                                                                               StartDatafeedAction.DatafeedParams params,
                                                                               DatafeedManager datafeedManager) {
        TransportStartDatafeedAction.DatafeedTask task = new TransportStartDatafeedAction.DatafeedTask(id, type, action, parentTaskId,
                params, Collections.emptyMap());
        task.datafeedManager = datafeedManager;
        return task;
    }
}
