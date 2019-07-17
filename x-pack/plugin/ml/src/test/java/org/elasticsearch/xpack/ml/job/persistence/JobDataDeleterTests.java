/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.arrayContaining;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JobDataDeleterTests extends ESTestCase {

    private static final String JOB_ID = "my-job-id";

    private Client client;

    @Before
    public void setUpTests() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testDeleteDatafeedTimingStats() {
        JobDataDeleter jobDataDeleter = new JobDataDeleter(client, JOB_ID);
        jobDataDeleter.deleteDatafeedTimingStats(ActionListener.wrap(
            deleteResponse -> {},
            e -> fail(e.toString())
        ));

        ArgumentCaptor<DeleteByQueryRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteByQueryRequest.class);
        verify(client).threadPool();
        verify(client).execute(eq(DeleteByQueryAction.INSTANCE), deleteRequestCaptor.capture(), any());
        verifyNoMoreInteractions(client);

        DeleteByQueryRequest deleteRequest = deleteRequestCaptor.getValue();
        assertThat(deleteRequest.indices(), arrayContaining(AnomalyDetectorsIndex.jobResultsAliasedName(JOB_ID)));
    }
}
