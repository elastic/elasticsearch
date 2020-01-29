/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsActionResponseTests;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask.StartingState;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFrameAnalyticsTaskTests extends ESTestCase {

    public void testDetermineStartingState_GivenZeroProgress() {
        List<PhaseProgress> progress = Arrays.asList(new PhaseProgress("reindexing", 0),
            new PhaseProgress("loading_data", 0),
            new PhaseProgress("analyzing", 0),
            new PhaseProgress("writing_results", 0));

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.FIRST_TIME));
    }

    public void testDetermineStartingState_GivenReindexingIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(new PhaseProgress("reindexing", 99),
            new PhaseProgress("loading_data", 0),
            new PhaseProgress("analyzing", 0),
            new PhaseProgress("writing_results", 0));

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_REINDEXING));
    }

    public void testDetermineStartingState_GivenLoadingDataIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 1),
            new PhaseProgress("analyzing", 0),
            new PhaseProgress("writing_results", 0));

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_ANALYZING));
    }

    public void testDetermineStartingState_GivenAnalyzingIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 99),
            new PhaseProgress("writing_results", 0));

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_ANALYZING));
    }

    public void testDetermineStartingState_GivenWritingResultsIsIncomplete() {
        List<PhaseProgress> progress = Arrays.asList(new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 100),
            new PhaseProgress("writing_results", 1));

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.RESUMING_ANALYZING));
    }

    public void testDetermineStartingState_GivenFinished() {
        List<PhaseProgress> progress = Arrays.asList(new PhaseProgress("reindexing", 100),
            new PhaseProgress("loading_data", 100),
            new PhaseProgress("analyzing", 100),
            new PhaseProgress("writing_results", 100));

        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", progress);

        assertThat(startingState, equalTo(StartingState.FINISHED));
    }

    public void testDetermineStartingState_GivenEmptyProgress() {
        StartingState startingState = DataFrameAnalyticsTask.determineStartingState("foo", Collections.emptyList());
        assertThat(startingState, equalTo(StartingState.FINISHED));
    }

    private void testPersistProgress(SearchHits searchHits, String expectedIndexOrAlias) {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);

        GetDataFrameAnalyticsStatsAction.Response getStatsResponse = GetDataFrameAnalyticsStatsActionResponseTests.randomResponse(1);
        doAnswer(withResponse(getStatsResponse)).when(client).execute(eq(GetDataFrameAnalyticsStatsAction.INSTANCE), any(), any());

        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        doAnswer(withResponse(searchResponse)).when(client).execute(eq(SearchAction.INSTANCE), any(), any());

        IndexResponse indexResponse = mock(IndexResponse.class);
        doAnswer(withResponse(indexResponse)).when(client).execute(eq(IndexAction.INSTANCE), any(), any());

        Runnable runnable = mock(Runnable.class);

        DataFrameAnalyticsTask.persistProgress(client, "task_id", runnable);

        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);

        InOrder inOrder = inOrder(client, runnable);
        inOrder.verify(client).execute(eq(GetDataFrameAnalyticsStatsAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(SearchAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(IndexAction.INSTANCE), indexRequestCaptor.capture(), any());
        inOrder.verify(runnable).run();
        inOrder.verifyNoMoreInteractions();

        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertThat(indexRequest.index(), equalTo(expectedIndexOrAlias));
        assertThat(indexRequest.id(), equalTo("data_frame_analytics-task_id-progress"));
    }

    public void testPersistProgress_ProgressDocumentCreated() {
        testPersistProgress(SearchHits.empty(), ".ml-state-write");
    }

    public void testPersistProgress_ProgressDocumentUpdated() {
        testPersistProgress(
            new SearchHits(new SearchHit[]{ SearchHit.createFromMap(Map.of("_index", ".ml-state-dummy")) }, null, 0.0f),
            ".ml-state-dummy");
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }
}
