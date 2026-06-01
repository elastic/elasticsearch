/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter.DatafeedTimingStatsPersister;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.TimeField;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScrollDataExtractorFactoryTests extends ESTestCase {

    private Client client;
    private DatafeedConfig datafeedConfig;
    private Job job;
    private TimeBasedExtractedFields extractedFields;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Before
    public void setUpTests() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        datafeedConfig = mock(DatafeedConfig.class);
        when(datafeedConfig.getHeaders()).thenReturn(Collections.emptyMap());
        job = mock(Job.class);
        when(job.getId()).thenReturn("factory-test-job");

        ExtractedField timeField = new TimeField("time", ExtractedField.Method.DOC_VALUE);
        extractedFields = new TimeBasedExtractedFields(
            timeField,
            Arrays.asList(timeField, new DocValueField("field_1", Collections.singleton("keyword")))
        );
        timingStatsReporter = new DatafeedTimingStatsReporter(
            new DatafeedTimingStats(job.getId()),
            mock(DatafeedTimingStatsPersister.class)
        );
    }

    private ScrollDataExtractorFactory newFactory() {
        return new ScrollDataExtractorFactory(
            client,
            datafeedConfig,
            null,
            job,
            extractedFields,
            NamedXContentRegistry.EMPTY,
            timingStatsReporter
        );
    }

    public void testCcsOrphanPastTtlShouldBeRemovedWithoutClearScrollCall() {
        ScrollDataExtractorFactory factory = newFactory();
        factory.orphanedScrolls.addLast(
            new ScrollDataExtractorFactory.OrphanedScroll(
                "scroll-id-old",
                System.currentTimeMillis() - ScrollDataExtractorFactory.ORPHAN_TTL_MILLIS - 1,
                0
            )
        );

        factory.retryClearOrphanedScrollIds();

        verify(client, never()).execute(same(TransportClearScrollAction.TYPE), any(ClearScrollRequest.class));
        assertThat(factory.orphanedScrolls.stream().anyMatch(o -> o.scrollId().equals("scroll-id-old")), is(false));
    }

    public void testCcsOrphanAtRetryLimitShouldBeRemovedWithoutClearScrollCall() {
        ScrollDataExtractorFactory factory = newFactory();
        factory.orphanedScrolls.addLast(
            new ScrollDataExtractorFactory.OrphanedScroll(
                "scroll-at-retry-limit",
                System.currentTimeMillis(),
                ScrollDataExtractorFactory.MAX_ORPHAN_RETRIES  // exactly at the limit: >= triggers eviction
            )
        );

        factory.retryClearOrphanedScrollIds();

        verify(client, never()).execute(same(TransportClearScrollAction.TYPE), any(ClearScrollRequest.class));
        assertThat(factory.orphanedScrolls.stream().anyMatch(o -> o.scrollId().equals("scroll-at-retry-limit")), is(false));
    }

    public void testCcsOrphanOverRetryLimitShouldBeRemovedWithoutClearScrollCall() {
        ScrollDataExtractorFactory factory = newFactory();
        factory.orphanedScrolls.addLast(
            new ScrollDataExtractorFactory.OrphanedScroll(
                "scroll-too-many-retries",
                System.currentTimeMillis(),
                ScrollDataExtractorFactory.MAX_ORPHAN_RETRIES + 1
            )
        );

        factory.retryClearOrphanedScrollIds();

        verify(client, never()).execute(same(TransportClearScrollAction.TYPE), any(ClearScrollRequest.class));
        assertThat(factory.orphanedScrolls.stream().anyMatch(o -> o.scrollId().equals("scroll-too-many-retries")), is(false));
    }

    public void testOrphanQueueAtCapacityShouldDropOldestOnEnqueue() {
        ScrollDataExtractorFactory factory = newFactory();
        for (int i = 0; i < ScrollDataExtractorFactory.MAX_ORPHAN_QUEUE_SIZE + 1; i++) {
            factory.addOrphanedScrollIds(List.of("scroll-" + i));
        }
        assertThat(factory.orphanedScrolls.size(), is(ScrollDataExtractorFactory.MAX_ORPHAN_QUEUE_SIZE));
        assertThat(factory.orphanedScrolls.stream().anyMatch(o -> o.scrollId().equals("scroll-0")), is(false));
        assertThat(
            factory.orphanedScrolls.stream()
                .anyMatch(o -> o.scrollId().equals("scroll-" + ScrollDataExtractorFactory.MAX_ORPHAN_QUEUE_SIZE)),
            is(true)
        );
    }

    @SuppressWarnings("unchecked")
    public void testCcsOrphanSuccessfullyClearedShouldBeRemovedFromQueue() {
        ScrollDataExtractorFactory factory = newFactory();
        factory.orphanedScrolls.addLast(new ScrollDataExtractorFactory.OrphanedScroll("scroll-recoverable", System.currentTimeMillis(), 0));

        ActionFuture<ClearScrollResponse> successFuture = mock(ActionFuture.class);
        when(successFuture.actionGet()).thenReturn(new ClearScrollResponse(true, 1));
        when(client.execute(same(TransportClearScrollAction.TYPE), any(ClearScrollRequest.class))).thenReturn((ActionFuture) successFuture);

        factory.retryClearOrphanedScrollIds();

        assertThat(factory.orphanedScrolls.isEmpty(), is(true));
    }
}
