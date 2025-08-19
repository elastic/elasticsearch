/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.IndexingOperationsMemoryRequirementsSampler.DEFAULT_SAMPLE_VALIDITY;
import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexingOperationsMemoryRequirementsSamplerTests extends ESTestCase {

    public void testMinimumHeapToProcessOperation() {
        var heapSize = ByteSizeValue.ofGb(randomIntBetween(1, 48));
        var heapSizeBytes = heapSize.getBytes();
        for (double maxDocSizeHeapPercent = 1; maxDocSizeHeapPercent < 100; maxDocSizeHeapPercent++) {
            var maxOperationSize = ByteSizeValue.ofBytes((long) (maxDocSizeHeapPercent / 100 * heapSize.getBytes()));
            var sampler = new IndexingOperationsMemoryRequirementsSampler(
                DEFAULT_SAMPLE_VALIDITY.nanos(),
                maxOperationSize.getBytes(),
                heapSize.getBytes(),
                new FakeTimeProvider(),
                null,
                null,
                () -> TransportVersion.fromName("indexing_operations_memory_requirements")
            );

            assertThat(
                sampler.minimumHeapRequiredToProcessOperation(randomLongBetween(0, maxOperationSize.getBytes())),
                is(lessThanOrEqualTo(heapSizeBytes))
            );

            assertThat(
                sampler.minimumHeapRequiredToProcessOperation(maxOperationSize.getBytes() + randomIntBetween(1, 1000)),
                is(greaterThan(heapSizeBytes))
            );
        }
    }

    public void testSamplesArePublishedPeriodicallyAndOnRejection() {
        var timeProvider = new FakeTimeProvider();
        var sentRequests = new ArrayDeque<TransportPublishIndexingOperationsHeapMemoryRequirements.Request>();

        var threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        var fakeClient = new NoOpClient(threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action.name(), equalTo(TransportPublishIndexingOperationsHeapMemoryRequirements.NAME));
                sentRequests.add((TransportPublishIndexingOperationsHeapMemoryRequirements.Request) request);
                listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
            }
        };
        final var currentHeapSize = ByteSizeValue.ofGb(randomIntBetween(1, 48)).getBytes();
        final long maxOperationSize = (long) (0.1 * currentHeapSize);
        var sampler = new IndexingOperationsMemoryRequirementsSampler(
            DEFAULT_SAMPLE_VALIDITY.nanos(),
            maxOperationSize,
            currentHeapSize,
            timeProvider,
            DIRECT_EXECUTOR_SERVICE,
            fakeClient,
            () -> TransportVersion.fromName("indexing_operations_memory_requirements")
        );

        // The sampler stores and publishes the largest operation size
        timeProvider.tick();
        var operationSize1 = randomLongBetween(2, maxOperationSize / 2);
        sampler.onPrimaryOperationTracked(operationSize1);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(operationSize1)));
        assertThat(sentRequests.peek(), is(notNullValue()));
        assertThat(sentRequests.poll().getMinimumRequiredHeapInBytes(), is(lessThanOrEqualTo(currentHeapSize)));

        // The sampler just stored a sample bigger than this new sample so this one would be ignored
        timeProvider.tick();
        var operationSize2 = randomLongBetween(1, operationSize1 - 1);
        sampler.onPrimaryOperationTracked(operationSize2);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(operationSize1)));
        assertThat(sentRequests.peek(), is(nullValue()));

        // The sampler just stored a sample bigger than this new sample so this one would be ignored
        timeProvider.tick();
        double newDocumentSizeDeltaBelowThreshold = IndexingOperationsMemoryRequirementsSampler.SIZE_INCREASE_DELTA_TO_RECORD_SAMPLE - 0.2;
        var operationSize3 = (long) (operationSize1 * newDocumentSizeDeltaBelowThreshold);
        sampler.onPrimaryOperationTracked(operationSize3);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(operationSize1)));
        assertThat(sentRequests.peek(), is(nullValue()));

        // The sampler just stored a sample bigger than this new sample so this one would be ignored
        timeProvider.tick();
        double newDocumentSizeDelta = IndexingOperationsMemoryRequirementsSampler.SIZE_INCREASE_DELTA_TO_RECORD_SAMPLE + 0.1;
        long operationSize4 = (long) (operationSize1 * newDocumentSizeDelta);
        sampler.onPrimaryOperationTracked(operationSize4);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(operationSize4)));
        assertThat(sentRequests.peek(), is(notNullValue()));
        assertThat(sentRequests.poll().getMinimumRequiredHeapInBytes(), is(lessThanOrEqualTo(currentHeapSize)));

        // The sampler got notified about a rejection, in that case it would store and publish the largest operation size right away
        timeProvider.tick();
        sampler.onLargeIndexingOperationRejection(maxOperationSize + 1);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(maxOperationSize + 1)));
        assertThat(sentRequests.peek(), is(notNullValue()));
        // Once an operation is rejected we should ask for more heap
        assertThat(sentRequests.poll().getMinimumRequiredHeapInBytes(), is(greaterThan(currentHeapSize)));

        // The sampler got notified about a new rejection of the same size,
        // in that case it would store and publish the largest operation size right away
        timeProvider.tick();
        sampler.onLargeIndexingOperationRejection(maxOperationSize + 1);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(maxOperationSize + 1)));
        assertThat(sentRequests.peek(), is(notNullValue()));
        // Once an operation is rejected we should ask for more heap
        assertThat(sentRequests.poll().getMinimumRequiredHeapInBytes(), is(greaterThan(currentHeapSize)));

        // A sample was stored recently and until SAMPLE_VALIDITY_IN_NANOS elapses, new values would be ignored
        timeProvider.tick();
        sampler.onPrimaryOperationTracked(12);
        assertThat(sentRequests.poll(), is(nullValue()));

        // A new operation rejection was sampled, therefore it's stored and published immediately
        timeProvider.tick();
        sampler.onLargeIndexingOperationRejection(maxOperationSize + 2);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(maxOperationSize + 2)));
        assertThat(sentRequests.peek(), is(notNullValue()));
        assertThat(sentRequests.poll().getMinimumRequiredHeapInBytes(), is(greaterThan(currentHeapSize)));

        // A new operation rejection is registered, but since it's smaller than the previous one, it won't be stored nor sent
        timeProvider.tick();
        sampler.onLargeIndexingOperationRejection(maxOperationSize + 1);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo(maxOperationSize + 2)));
        assertThat(sentRequests.peek(), is(nullValue()));

        timeProvider.tick();
        sampler.onPrimaryOperationTracked(randomIntBetween(0, 120));
        assertThat(sentRequests.poll(), is(nullValue()));

        // Once SAMPLE_VALIDITY_IN_NANOS has elapsed a new sample would be stored and published
        timeProvider.advanceClock(DEFAULT_SAMPLE_VALIDITY.nanos());
        int largestOperationSizeInBytes = randomIntBetween(0, 120);
        sampler.onPrimaryOperationTracked(largestOperationSizeInBytes);
        assertThat(sampler.getLargestOperationSizeInBytes(), is(equalTo((long) largestOperationSizeInBytes)));
        assertThat(sentRequests.peek(), is(notNullValue()));
        assertThat(sentRequests.poll().getMinimumRequiredHeapInBytes(), is(lessThanOrEqualTo(currentHeapSize)));
    }

    static class FakeTimeProvider implements TimeProvider {
        final AtomicLong fakeClock = new AtomicLong();

        @Override
        public long relativeTimeInMillis() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long relativeTimeInNanos() {
            return fakeClock.get();
        }

        @Override
        public long rawRelativeTimeInMillis() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long absoluteTimeInMillis() {
            throw new UnsupportedOperationException();
        }

        void tick() {
            advanceClock(1);
        }

        void advanceClock(long nanos) {
            fakeClock.addAndGet(nanos);
        }
    }
}
