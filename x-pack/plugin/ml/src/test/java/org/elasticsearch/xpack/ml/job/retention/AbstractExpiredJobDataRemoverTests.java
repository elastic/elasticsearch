/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.Before;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractExpiredJobDataRemoverTests extends ESTestCase {

    // We can't test an abstract class so make a concrete class
    // as simple as possible
    private static class ConcreteExpiredJobDataRemover extends AbstractExpiredJobDataRemover {

        private int getRetentionDaysCallCount = 0;

        ConcreteExpiredJobDataRemover(OriginSettingClient client, Iterator<Job> jobIterator) {
            super(client, jobIterator);
        }

        @Override
        protected Long getRetentionDays(Job job) {
            getRetentionDaysCallCount++;
            // cover both code paths
            return randomBoolean() ? null : 0L;
        }

        @Override
        void calcCutoffEpochMs(String jobId, long retentionDays, ActionListener<CutoffDetails> listener) {
            long nowEpochMs = Instant.now(Clock.systemDefaultZone()).toEpochMilli();
            listener.onResponse(new CutoffDetails(nowEpochMs, nowEpochMs - new TimeValue(retentionDays, TimeUnit.DAYS).getMillis()));
        }

        @Override
        protected void removeDataBefore(
            Job job,
            float requestsPerSec,
            long latestTimeMs,
            long cutoffEpochMs,
            ActionListener<Boolean> listener
        ) {
            listener.onResponse(Boolean.TRUE);
        }
    }

    private OriginSettingClient originSettingClient;

    @Before
    public void setUpTests() {
        Client client = mock(Client.class);
        originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
    }

    static SearchResponse createSearchResponse(List<? extends ToXContent> toXContents) throws IOException {
        return createSearchResponse(toXContents, toXContents.size());
    }

    static SearchResponse createSearchResponseFromHits(List<SearchHit> hits) {
        SearchHits searchHits = new SearchHits(hits.toArray(new SearchHit[]{}),
            new TotalHits(hits.size(), TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(searchHits);
        return searchResponse;
    }

    private static SearchResponse createSearchResponse(List<? extends ToXContent> toXContents, int totalHits) throws IOException {
        SearchHit[] hitsArray = new SearchHit[toXContents.size()];
        for (int i = 0; i < toXContents.size(); i++) {
            hitsArray[i] = new SearchHit(randomInt());
            XContentBuilder jsonBuilder = JsonXContent.contentBuilder();
            toXContents.get(i).toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            hitsArray[i].sourceRef(BytesReference.bytes(jsonBuilder));
        }
        SearchHits hits = new SearchHits(hitsArray, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = mock(SearchResponse.class);
        when(searchResponse.getHits()).thenReturn(hits);
        return searchResponse;
    }

    public void testRemoveGivenNoJobs() {
        TestListener listener = new TestListener();
        Iterator<Job> jobIterator = Collections.emptyIterator();
        ConcreteExpiredJobDataRemover remover = new ConcreteExpiredJobDataRemover(originSettingClient, jobIterator);
        remover.remove(1.0f, listener, () -> false);

        listener.waitToCompletion();
        assertThat(listener.success, is(true));
        assertEquals(0, remover.getRetentionDaysCallCount);
    }

    public void testRemoveGivenTimeOut() {

        int totalHits = 3;
        List<Job> jobs = Arrays.asList(
            JobTests.buildJobBuilder("job1").build(),
            JobTests.buildJobBuilder("job2").build(),
            JobTests.buildJobBuilder("job3").build()
        );

        final int timeoutAfter = randomIntBetween(0, totalHits - 1);
        AtomicInteger attemptsLeft = new AtomicInteger(timeoutAfter);

        TestListener listener = new TestListener();
        ConcreteExpiredJobDataRemover remover = new ConcreteExpiredJobDataRemover(originSettingClient, jobs.iterator());
        remover.remove(1.0f, listener, () -> attemptsLeft.getAndDecrement() <= 0);

        listener.waitToCompletion();
        assertThat(listener.success, is(false));
        assertEquals(timeoutAfter, remover.getRetentionDaysCallCount);
    }

    static class TestListener implements ActionListener<Boolean> {

        boolean success;
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void onResponse(Boolean aBoolean) {
            success = aBoolean;
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            latch.countDown();
        }

        void waitToCompletion() {
            try {
                latch.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                fail("listener timed out before completing");
            }
        }
    }

}
