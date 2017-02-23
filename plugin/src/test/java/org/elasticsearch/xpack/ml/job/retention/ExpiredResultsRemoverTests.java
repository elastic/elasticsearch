/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.mock.orig.Mockito;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.MlDeleteByQueryAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobTests;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpiredResultsRemoverTests extends ESTestCase {

    private Client client;
    private ClusterService clusterService;
    private ClusterState clusterState;
    private List<DeleteByQueryRequest> capturedDeleteByQueryRequests;

    @Before
    public void setUpTests() {
        capturedDeleteByQueryRequests = new ArrayList<>();
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        client = mock(Client.class);
        doAnswer(new Answer<Void>() {
                 @Override
                 public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                     capturedDeleteByQueryRequests.add((DeleteByQueryRequest) invocationOnMock.getArguments()[1]);
                     ActionListener<BulkByScrollResponse> listener =
                             (ActionListener<BulkByScrollResponse>) invocationOnMock.getArguments()[2];
                     listener.onResponse(null);
                     return null;
                 }
             }).when(client).execute(same(MlDeleteByQueryAction.INSTANCE), any(), any());
    }

    public void testOnTrigger_GivenNoJobs() {
        givenClientRequestsSucceed();
        givenJobs(Collections.emptyList());

        createExpiredResultsRemover().onTrigger();

        Mockito.verifyNoMoreInteractions(client);
    }

    public void testOnTrigger_GivenJobsWithoutRetentionPolicy() {
        givenClientRequestsSucceed();
        givenJobs(Arrays.asList(
                JobTests.buildJobBuilder("foo").build(),
                JobTests.buildJobBuilder("bar").build()
        ));

        createExpiredResultsRemover().onTrigger();

        Mockito.verifyNoMoreInteractions(client);
    }

    public void testOnTrigger_GivenJobsWithAndWithoutRetentionPolicy() throws IOException {
        givenClientRequestsSucceed();
        givenJobs(Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
                JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        createExpiredResultsRemover().onTrigger();

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(2));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsIndexName("results-1")}));
        dbqRequest = capturedDeleteByQueryRequests.get(1);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsIndexName("results-2")}));
    }

    public void testOnTrigger_GivenClientRequestsFailed_StillIteratesThroughJobs() throws IOException {
        givenClientRequestsFailed();
        givenJobs(Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
                JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        createExpiredResultsRemover().onTrigger();

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(2));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsIndexName("results-1")}));
        dbqRequest = capturedDeleteByQueryRequests.get(1);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsIndexName("results-2")}));
    }

    private void givenClientRequestsSucceed() {
        givenClientRequests(true);
    }

    private void givenClientRequestsFailed() {
        givenClientRequests(false);
    }

    private void givenClientRequests(boolean shouldSucceed) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                capturedDeleteByQueryRequests.add((DeleteByQueryRequest) invocationOnMock.getArguments()[1]);
                ActionListener<BulkByScrollResponse> listener =
                        (ActionListener<BulkByScrollResponse>) invocationOnMock.getArguments()[2];
                if (shouldSucceed) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new RuntimeException("failed"));
                }
                return null;
            }
        }).when(client).execute(same(MlDeleteByQueryAction.INSTANCE), any(), any());
    }

    private void givenJobs(List<Job> jobs) {
        Map<String, Job> jobsMap = new HashMap<>();
        jobs.stream().forEach(job -> jobsMap.put(job.getId(), job));
        MlMetadata mlMetadata = mock(MlMetadata.class);
        when(mlMetadata.getJobs()).thenReturn(jobsMap);
        MetaData metadata = mock(MetaData.class);
        when(metadata.custom(MlMetadata.TYPE)).thenReturn(mlMetadata);
        when(clusterState.getMetaData()).thenReturn(metadata);
    }

    private ExpiredResultsRemover createExpiredResultsRemover() {
        return new ExpiredResultsRemover(client, clusterService, jobId -> mock(Auditor.class));
    }
}