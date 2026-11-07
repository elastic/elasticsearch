/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.job;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.search.TotalHits.Relation.EQUAL_TO;
import static org.elasticsearch.xpack.ml.job.persistence.JobScopedDeleteCheckTests.assertNoMissingJobScopeEvent;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeleteForecastActionTests extends ESTestCase {

    private static final int TEST_RUNS = 10;

    public void testValidateForecastStateWithAllFailedFinished() {
        for (int i = 0; i < TEST_RUNS; ++i) {
            List<SearchHit> forecastRequestStatsHits = Stream.generate(
                () -> createForecastStatsHit(
                    randomFrom(ForecastRequestStats.ForecastRequestStatus.FAILED, ForecastRequestStats.ForecastRequestStatus.FINISHED)
                )
            ).limit(randomInt(10)).collect(Collectors.toList());

            try {
                // This should not throw.
                TransportDeleteForecastAction.extractForecastIds(
                    forecastRequestStatsHits.toArray(SearchHits.EMPTY),
                    randomFrom(JobState.values()),
                    randomAlphaOfLength(10)
                );
            } finally {
                for (SearchHit hit : forecastRequestStatsHits) {
                    hit.decRef();
                }
            }
        }
    }

    public void testValidateForecastStateWithSomeFailedFinished() {
        for (int i = 0; i < TEST_RUNS; ++i) {
            List<SearchHit> forecastRequestStatsHits = Stream.generate(
                () -> createForecastStatsHit(randomFrom(ForecastRequestStats.ForecastRequestStatus.values()))
            ).limit(randomInt(10)).collect(Collectors.toList());

            forecastRequestStatsHits.add(createForecastStatsHit(ForecastRequestStats.ForecastRequestStatus.STARTED));

            try {
                {
                    JobState jobState = randomFrom(JobState.CLOSED, JobState.CLOSING, JobState.FAILED);
                    try {
                        TransportDeleteForecastAction.extractForecastIds(
                            forecastRequestStatsHits.toArray(SearchHits.EMPTY),
                            jobState,
                            randomAlphaOfLength(10)
                        );
                    } catch (Exception ex) {
                        fail("Should not have thrown: " + ex.getMessage());
                    }
                }
                {
                    JobState jobState = JobState.OPENED;
                    expectThrows(
                        ElasticsearchStatusException.class,
                        () -> TransportDeleteForecastAction.extractForecastIds(
                            forecastRequestStatsHits.toArray(SearchHits.EMPTY),
                            jobState,
                            randomAlphaOfLength(10)
                        )
                    );
                }
            } finally {
                for (SearchHit hit : forecastRequestStatsHits) {
                    hit.decRef();
                }
            }
        }
    }

    public void testDeleteForecastShouldNotLogMissingJobScope() {
        String jobId = "forecast-job";
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        List<DeleteByQueryRequest> deleteByQueryRequests = new ArrayList<>();
        SearchHit hit = createForecastStatsHit(ForecastRequestStats.ForecastRequestStatus.FINISHED);
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, EQUAL_TO), 1f);
        Client client = new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof SearchRequest) {
                    SearchResponse searchResponse = mock(SearchResponse.class);
                    when(searchResponse.getHits()).thenReturn(searchHits);
                    listener.onResponse((Response) searchResponse);
                } else if (request instanceof DeleteByQueryRequest deleteByQueryRequest) {
                    deleteByQueryRequests.add(deleteByQueryRequest);
                    listener.onResponse(
                        (Response) new BulkByPaginatedSearchResponse(
                            TimeValue.ZERO,
                            new BulkByPaginatedSearchTask.Status(Collections.emptyList(), null, 0f),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            false
                        )
                    );
                } else {
                    throw new AssertionError("unexpected request [" + request.getClass().getSimpleName() + "]");
                }
            }
        };
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        TransportDeleteForecastAction action = new TransportDeleteForecastAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService
        );

        try {
            assertNoMissingJobScopeEvent(
                () -> action.doExecute(
                    null,
                    new DeleteForecastAction.Request(jobId, "_all"),
                    ActionTestUtils.assertNoFailureListener(response -> {})
                )
            );
        } finally {
            searchHits.decRef();
        }

        assertThat(deleteByQueryRequests, hasSize(1));
    }

    private static SearchHit createForecastStatsHit(ForecastRequestStats.ForecastRequestStatus status) {
        Map<String, DocumentField> documentFields = Maps.newMapWithExpectedSize(2);
        documentFields.put(
            ForecastRequestStats.FORECAST_ID.getPreferredName(),
            new DocumentField(ForecastRequestStats.FORECAST_ID.getPreferredName(), Collections.singletonList(""))
        );
        documentFields.put(
            ForecastRequestStats.STATUS.getPreferredName(),
            new DocumentField(ForecastRequestStats.STATUS.getPreferredName(), Collections.singletonList(status.toString()))
        );
        SearchHit hit = new SearchHit(0, "");
        hit.addDocumentFields(documentFields, Map.of());
        return hit;
    }
}
