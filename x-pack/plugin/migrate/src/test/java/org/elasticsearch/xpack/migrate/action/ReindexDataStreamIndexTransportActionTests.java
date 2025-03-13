/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class ReindexDataStreamIndexTransportActionTests extends ESTestCase {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private TransportService transportService;
    @Mock
    private ClusterService clusterService;
    @Mock
    private ActionFilters actionFilters;
    @Mock
    private Client client;

    @InjectMocks
    private ReindexDataStreamIndexTransportAction action;

    @Captor
    private ArgumentCaptor<ReindexRequest> request;

    private AutoCloseable mocks;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        mocks.close();
    }

    public void testGenerateDestIndexName_noDotPrefix() {
        String sourceIndex = "sourceindex";
        String expectedDestIndex = "migrated-sourceindex";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testGenerateDestIndexName_withDotPrefix() {
        String sourceIndex = ".sourceindex";
        String expectedDestIndex = ".migrated-sourceindex";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testGenerateDestIndexName_withHyphen() {
        String sourceIndex = "source-index";
        String expectedDestIndex = "migrated-source-index";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testGenerateDestIndexName_withUnderscore() {
        String sourceIndex = "source_index";
        String expectedDestIndex = "migrated-source_index";
        String actualDestIndex = ReindexDataStreamIndexTransportAction.generateDestIndexName(sourceIndex);
        assertEquals(expectedDestIndex, actualDestIndex);
    }

    public void testReindexIncludesRateLimit() {
        var targetRateLimit = randomFloatBetween(1, 100, true);
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), targetRateLimit)
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        doNothing().when(client).execute(eq(ReindexAction.INSTANCE), request.capture(), any());

        action.reindex(sourceIndex, destIndex, listener, taskId);

        ReindexRequest requestValue = request.getValue();

        assertEquals(targetRateLimit, requestValue.getRequestsPerSecond(), 0.0);
    }

    public void testReindexIncludesInfiniteRateLimit() {
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), "-1")
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );
        doNothing().when(client).execute(eq(ReindexAction.INSTANCE), request.capture(), any());

        action.reindex(sourceIndex, destIndex, listener, taskId);

        ReindexRequest requestValue = request.getValue();

        assertEquals(Float.POSITIVE_INFINITY, requestValue.getRequestsPerSecond(), 0.0);
    }

    public void testReindexZeroRateLimitThrowsError() {
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), "0")
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.reindex(sourceIndex, destIndex, listener, taskId)
        );
        assertEquals(
            "Failed to parse value [0.0] for setting [migrate.data_stream_reindex_max_request_per_second]"
                + " must be greater than 0 or -1 for infinite",
            e.getMessage()
        );
    }

    public void testReindexNegativeRateLimitThrowsError() {
        float targetRateLimit = randomFloatBetween(-100, -1, true);
        Settings settings = Settings.builder()
            .put(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING.getKey(), targetRateLimit)
            .build();

        String sourceIndex = randomAlphanumericOfLength(10);
        String destIndex = randomAlphanumericOfLength(10);
        ActionListener<BulkByScrollResponse> listener = ActionListener.noop();
        TaskId taskId = TaskId.EMPTY_TASK_ID;

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(
                settings,
                Collections.singleton(ReindexDataStreamIndexTransportAction.REINDEX_MAX_REQUESTS_PER_SECOND_SETTING)
            )
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.reindex(sourceIndex, destIndex, listener, taskId)
        );
        assertEquals(
            "Failed to parse value ["
                + targetRateLimit
                + "] for setting [migrate.data_stream_reindex_max_request_per_second]"
                + " must be greater than 0 or -1 for infinite",
            e.getMessage()
        );
    }
}
