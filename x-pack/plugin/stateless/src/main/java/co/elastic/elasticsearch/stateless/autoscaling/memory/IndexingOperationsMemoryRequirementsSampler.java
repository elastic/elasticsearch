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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressureMonitor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This class monitors the memory consumed by indexing operations, tracks the largest
 * operations, and publishes memory requirement information to help with autoscaling decisions.
 * It periodically captures the largest operation size observed within a configurable time window
 * and calculates the minimum heap size required to process operations of that size.
 */
public class IndexingOperationsMemoryRequirementsSampler implements IndexingPressureMonitor.IndexingPressureListener {
    public static final TimeValue DEFAULT_SAMPLE_VALIDITY = TimeValue.timeValueMinutes(1);
    public static final Setting<TimeValue> SAMPLE_VALIDITY_SETTING = Setting.timeSetting(
        "serverless.autoscaling.indexing_memory_requirements_sampler.largest_indexing_operation_sample.validity",
        DEFAULT_SAMPLE_VALIDITY,
        Setting.Property.NodeScope
    );

    /**
     * Threshold multiplier that determines when to record a new sample.
     * A new sample is recorded only when the current operation size exceeds
     * the previous largest operation size by at least 25%.
     * This prevents recording samples for minor size fluctuations.
     */
    public static final double SIZE_INCREASE_DELTA_TO_RECORD_SAMPLE = 1.25;

    private final Logger logger = LogManager.getLogger(IndexingOperationsMemoryRequirementsSampler.class);

    private static final TransportVersion INDEXING_OPERATIONS_MEMORY_REQUIREMENTS = TransportVersion.fromName(
        "indexing_operations_memory_requirements"
    );

    private final long sampleValidityInNanos;
    /**
     * The maximum ratio of heap that can be used for a single indexing operation.
     * This is used to calculate the minimum heap size required for operations.
     */
    private final double maxHeapRatioForIndexingOperations;
    private final TimeProvider timeProvider;
    private final Executor publishExecutor;
    private final Client client;
    private final AtomicReference<MaxOperationSize> maxOperationSizeRef = new AtomicReference<>();
    private final Supplier<TransportVersion> minTransportVersionSupplier;

    public IndexingOperationsMemoryRequirementsSampler(
        Settings settings,
        ThreadPool threadPool,
        Client client,
        Supplier<TransportVersion> minTransportVersionSupplier,
        long maxAllowedOperationSizeInBytes
    ) {
        this(
            SAMPLE_VALIDITY_SETTING.get(settings).nanos(),
            maxAllowedOperationSizeInBytes,
            JvmInfo.jvmInfo().getMem().getHeapMax().getBytes(),
            threadPool,
            threadPool.generic(),
            client,
            minTransportVersionSupplier
        );
    }

    IndexingOperationsMemoryRequirementsSampler(
        long sampleValidityInNanos,
        long maxOperationSizeInBytes,
        long currentHeapInBytes,
        TimeProvider timeProvider,
        Executor publishExecutor,
        Client client,
        Supplier<TransportVersion> minTransportVersionSupplier
    ) {
        this.sampleValidityInNanos = sampleValidityInNanos;
        this.minTransportVersionSupplier = minTransportVersionSupplier;
        // For this to trigger an autoscaling event, the max operation size must be configured as a ratio (by default a 10%)
        // Otherwise even if the autoscaler scales up, the operations will be rejected as the max operation size is an absolute value.
        this.maxHeapRatioForIndexingOperations = (double) maxOperationSizeInBytes / (double) currentHeapInBytes;
        this.timeProvider = timeProvider;
        this.publishExecutor = publishExecutor;
        this.client = client;
    }

    @Override
    public void onPrimaryOperationTracked(long largestOperationSizeInBytes) {
        if (shouldRecordSample(largestOperationSizeInBytes)) {
            if (maybeRecordSample(largestOperationSizeInBytes)) {
                publishLatestSample();
            }
        }
    }

    @Override
    public void onLargeIndexingOperationRejection(long largestOperationSizeInBytes) {
        // If the IndexingPressure mechanism rejected an operation due to being larger than the max size we should publish the
        // sample right away to trigger an autoscaling event.
        if (maybeRecordSample(largestOperationSizeInBytes)) {
            publishLatestSample();
        }
    }

    private boolean shouldRecordSample(long largestOperationSizeInBytes) {
        var currentMaxOperationSize = maxOperationSizeRef.get();
        return currentMaxOperationSize == null
            || currentMaxOperationSize.largestOperationSize() * SIZE_INCREASE_DELTA_TO_RECORD_SAMPLE <= largestOperationSizeInBytes
            || currentMaxOperationSize.validUntil() < relativeTimeInNanos();
    }

    /**
     * Records a new operation size sample if it's larger than the current one or if the current one is stale.
     *
     * @return true if the new sample was recorded, false if the existing sample was kept
     */
    private boolean maybeRecordSample(long largestOperationSize) {
        long now = relativeTimeInNanos();
        var proposedNewSample = new MaxOperationSize(largestOperationSize, now + sampleValidityInNanos);
        return maxOperationSizeRef.accumulateAndGet(proposedNewSample, (existing, proposed) -> {
            if (existing == null || existing.validUntil() < now) {
                return proposed;
            }
            return existing.compareTo(proposed) >= 0 ? existing : proposed;
        }) == proposedNewSample;
    }

    private void publishLatestSample() {
        if (minTransportVersionSupplier.get().supports(INDEXING_OPERATIONS_MEMORY_REQUIREMENTS)) {
            publishExecutor.execute(() -> {
                MaxOperationSize maxOperationSize = maxOperationSizeRef.get();
                ThreadContext threadContext = client.threadPool().getThreadContext();
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    threadContext.markAsSystemContext();
                    long minimumRequiredHeapInBytes = minimumHeapRequiredToProcessOperation(maxOperationSize.largestOperationSize);
                    client.execute(
                        TransportPublishIndexingOperationsHeapMemoryRequirements.INSTANCE,
                        new TransportPublishIndexingOperationsHeapMemoryRequirements.Request(minimumRequiredHeapInBytes),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(ActionResponse.Empty empty) {

                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("Failed to publish indexing operations heap memory requirements", e);
                            }
                        }
                    );
                }
            });
        }
    }

    private long relativeTimeInNanos() {
        return timeProvider.relativeTimeInNanos();
    }

    // Visible for testing
    long minimumHeapRequiredToProcessOperation(long operationSize) {
        if (maxHeapRatioForIndexingOperations == 0) {
            return 0;
        }
        return (long) Math.ceil(operationSize / maxHeapRatioForIndexingOperations);
    }

    // Visible for testing
    long getLargestOperationSizeInBytes() {
        var maxOperationSize = maxOperationSizeRef.get();
        return maxOperationSize == null ? 0 : maxOperationSize.largestOperationSize();
    }

    private record MaxOperationSize(long largestOperationSize, long validUntil) implements Comparable<MaxOperationSize> {
        private static final Comparator<MaxOperationSize> COMPARATOR = Comparator.comparingLong(MaxOperationSize::largestOperationSize)
            .thenComparingLong(MaxOperationSize::validUntil);

        @Override
        public int compareTo(MaxOperationSize o) {
            return COMPARATOR.compare(this, o);
        }
    }
}
