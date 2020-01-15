/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExpiredResultsRemoverTests extends ESTestCase {

    private Client client;
    private OriginSettingClient originSettingClient;
    private List<DeleteByQueryRequest> capturedDeleteByQueryRequests;
    private ActionListener<Boolean> listener;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpTests() {
        capturedDeleteByQueryRequests = new ArrayList<>();

        client = org.mockito.Mockito.mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        listener = mock(ActionListener.class);
    }

    public void testRemove_GivenNoJobs() throws IOException {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client, Collections.emptyList());

        createExpiredResultsRemover().remove(listener, () -> false);

        verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
        verify(listener).onResponse(true);
    }

    public void testRemove_GivenJobsWithoutRetentionPolicy() throws IOException {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
                JobTests.buildJobBuilder("foo").build(),
                JobTests.buildJobBuilder("bar").build()
        ));

        createExpiredResultsRemover().remove(listener, () -> false);

        verify(listener).onResponse(true);
        verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
    }

    public void testRemove_GivenJobsWithAndWithoutRetentionPolicy() throws Exception {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
                JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        createExpiredResultsRemover().remove(listener, () -> false);

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(2));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("results-1")}));
        dbqRequest = capturedDeleteByQueryRequests.get(1);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("results-2")}));
        verify(listener).onResponse(true);
    }

    public void testRemove_GivenTimeout() throws Exception {
        givenClientRequestsSucceed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
            JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
            JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        final int timeoutAfter = randomIntBetween(0, 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        createExpiredResultsRemover().remove(listener, () -> (attemptsLeft.getAndDecrement() <= 0));

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(timeoutAfter));
        verify(listener).onResponse(false);
    }

    public void testRemove_GivenClientRequestsFailed() throws IOException {
        givenClientRequestsFailed();
        AbstractExpiredJobDataRemoverTests.givenJobs(client,
                Arrays.asList(
                JobTests.buildJobBuilder("none").build(),
                JobTests.buildJobBuilder("results-1").setResultsRetentionDays(10L).build(),
                JobTests.buildJobBuilder("results-2").setResultsRetentionDays(20L).build()
        ));

        createExpiredResultsRemover().remove(listener, () -> false);

        assertThat(capturedDeleteByQueryRequests.size(), equalTo(1));
        DeleteByQueryRequest dbqRequest = capturedDeleteByQueryRequests.get(0);
        assertThat(dbqRequest.indices(), equalTo(new String[] {AnomalyDetectorsIndex.jobResultsAliasedName("results-1")}));
        verify(listener).onFailure(any());
    }

    private void givenClientRequestsSucceed() {
        givenClientRequests(true);
    }

    private void givenClientRequestsFailed() {
        givenClientRequests(false);
    }

    @SuppressWarnings("unchecked")
    private void givenClientRequests(boolean shouldSucceed) {
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) {
                capturedDeleteByQueryRequests.add((DeleteByQueryRequest) invocationOnMock.getArguments()[1]);
                ActionListener<BulkByScrollResponse> listener =
                        (ActionListener<BulkByScrollResponse>) invocationOnMock.getArguments()[2];
                if (shouldSucceed) {
                    BulkByScrollResponse bulkByScrollResponse = mock(BulkByScrollResponse.class);
                    when(bulkByScrollResponse.getDeleted()).thenReturn(42L);
                    listener.onResponse(bulkByScrollResponse);
                } else {
                    listener.onFailure(new RuntimeException("failed"));
                }
                return null;
            }
        }).when(client).execute(same(DeleteByQueryAction.INSTANCE), any(), any());
    }

    private ExpiredResultsRemover createExpiredResultsRemover() {
        return new ExpiredResultsRemover(originSettingClient, mock(AnomalyDetectionAuditor.class));
    }
}
