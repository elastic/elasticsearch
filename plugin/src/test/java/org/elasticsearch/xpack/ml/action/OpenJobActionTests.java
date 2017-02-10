/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.net.InetAddress;
import java.util.Collections;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;

public class OpenJobActionTests extends ESTestCase {

    public void testValidate() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id").build(), false);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name", "_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTaskInProgress<OpenJobAction.Request> task =
                new PersistentTaskInProgress<>(1L, OpenJobAction.NAME, new OpenJobAction.Request("job_id"), "_node_id");
        task = new PersistentTaskInProgress<>(task, randomFrom(JobState.CLOSED, JobState.FAILED));
        PersistentTasksInProgress tasks = new PersistentTasksInProgress(1L, Collections.singletonMap(1L, task));

        OpenJobAction.validate("job_id", mlBuilder.build(), tasks, nodes);
        OpenJobAction.validate("job_id", mlBuilder.build(), new PersistentTasksInProgress(1L, Collections.emptyMap()), nodes);
        OpenJobAction.validate("job_id", mlBuilder.build(), null, nodes);

        task = new PersistentTaskInProgress<>(1L, OpenJobAction.NAME, new OpenJobAction.Request("job_id"), "_other_node_id");
        task = new PersistentTaskInProgress<>(task, JobState.OPENED);
        tasks = new PersistentTasksInProgress(1L, Collections.singletonMap(1L, task));
        OpenJobAction.validate("job_id", mlBuilder.build(), tasks, nodes);
    }

    public void testValidate_jobMissing() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id1").build(), false);
        expectThrows(ResourceNotFoundException.class, () -> OpenJobAction.validate("job_id2", mlBuilder.build(), null, null));
    }

    public void testValidate_jobMarkedAsDeleted() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        Job.Builder jobBuilder = buildJobBuilder("job_id");
        jobBuilder.setDeleted(true);
        mlBuilder.putJob(jobBuilder.build(), false);
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> OpenJobAction.validate("job_id", mlBuilder.build(), null, null));
        assertEquals("Cannot open job [job_id] because it has been marked as deleted", e.getMessage());
    }

    public void testValidate_unexpectedState() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id").build(), false);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("_node_name", "_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTaskInProgress<OpenJobAction.Request> task =
                new PersistentTaskInProgress<>(1L, OpenJobAction.NAME, new OpenJobAction.Request("job_id"), "_node_id");
        JobState jobState = randomFrom(JobState.OPENING, JobState.OPENED, JobState.CLOSING);
        task = new PersistentTaskInProgress<>(task, jobState);
        PersistentTasksInProgress tasks1 = new PersistentTasksInProgress(1L, Collections.singletonMap(1L, task));

        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> OpenJobAction.validate("job_id", mlBuilder.build(), tasks1, nodes));
        assertEquals("[job_id] expected state [closed] or [failed], but got [" + jobState +"]", e.getMessage());

        task = new PersistentTaskInProgress<>(1L, OpenJobAction.NAME, new OpenJobAction.Request("job_id"), "_other_node_id");
        jobState = randomFrom(JobState.OPENING, JobState.CLOSING);
        task = new PersistentTaskInProgress<>(task, jobState);
        PersistentTasksInProgress tasks2 = new PersistentTasksInProgress(1L, Collections.singletonMap(1L, task));

        e = expectThrows(ElasticsearchStatusException.class,
                () -> OpenJobAction.validate("job_id", mlBuilder.build(), tasks2, nodes));
        assertEquals("[job_id] expected state [closed] or [failed], but got [" + jobState +"]", e.getMessage());
    }

}
