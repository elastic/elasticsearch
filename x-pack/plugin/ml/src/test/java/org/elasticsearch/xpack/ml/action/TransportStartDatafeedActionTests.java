/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunnerTests;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Collections;
import java.util.Date;

import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT;
import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TransportStartDatafeedActionTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testValidate_jobClosed() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        PersistentTasksCustomMetadata tasks = PersistentTasksCustomMetadata.builder().build();
        DatafeedConfig datafeedConfig1 = DatafeedRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        Exception e = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportStartDatafeedAction.validate(job1, datafeedConfig1, tasks, xContentRegistry())
        );
        assertThat(e.getMessage(), equalTo("cannot start datafeed [foo-datafeed] because job [job_id] is closed"));
    }

    public void testValidate_jobOpening() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), null, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();

        TransportStartDatafeedAction.validate(job1, datafeedConfig1, tasks, xContentRegistry());
    }

    public void testValidate_jobOpened() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();

        TransportStartDatafeedAction.validate(job1, datafeedConfig1, tasks, xContentRegistry());
    }

    public void testDeprecationsLogged() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        DatafeedConfig.Builder datafeedConfig = DatafeedRunnerTests.createDatafeedConfig("start-data-feed-test", job1.getId());
        DatafeedConfig config = spy(datafeedConfig.build());
        doReturn(Collections.singletonList("Deprecated Agg")).when(config).getAggDeprecations(any(NamedXContentRegistry.class));
        doReturn(Collections.singletonList("Deprecated Query")).when(config).getQueryDeprecations(any(NamedXContentRegistry.class));

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);

        TransportStartDatafeedAction.auditDeprecations(config, job1, auditor, xContentRegistry());

        verify(auditor).warning(
            job1.getId(),
            "datafeed [start-data-feed-test] configuration has deprecations. [Deprecated Agg, Deprecated Query]"
        );
    }

    public void testNoDeprecationsLogged() {
        Job job1 = DatafeedRunnerTests.createDatafeedJob().build(new Date());
        DatafeedConfig.Builder datafeedConfig = DatafeedRunnerTests.createDatafeedConfig("start-data-feed-test", job1.getId());
        DatafeedConfig config = spy(datafeedConfig.build());
        doReturn(Collections.emptyList()).when(config).getAggDeprecations(any(NamedXContentRegistry.class));
        doReturn(Collections.emptyList()).when(config).getQueryDeprecations(any(NamedXContentRegistry.class));

        AnomalyDetectionAuditor auditor = mock(AnomalyDetectionAuditor.class);

        TransportStartDatafeedAction.auditDeprecations(config, job1, auditor, xContentRegistry());

        verify(auditor, never()).warning(any(), any());
    }

    public static TransportStartDatafeedAction.DatafeedTask createDatafeedTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        StartDatafeedAction.DatafeedParams params,
        DatafeedRunner datafeedRunner
    ) {
        TransportStartDatafeedAction.DatafeedTask task = new TransportStartDatafeedAction.DatafeedTask(
            id,
            type,
            action,
            parentTaskId,
            params,
            Collections.emptyMap()
        );
        assertThat(task.setDatafeedRunner(datafeedRunner), is(TransportStartDatafeedAction.DatafeedTask.StoppedOrIsolated.NEITHER));
        return task;
    }
}
