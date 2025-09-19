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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.engine.MergeEventListener;
import org.elasticsearch.index.merge.OnGoingMerge;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Tracks the currently queued or running shard merge with the highest memory usage estimate and ensures that upon each new merge being
 * queued or finished, a new publication is sent with the current known max memory usage estimate of the non-finished merges. The
 * publication with the highest seq no for each node, is the latest known max memory usage estimate of shard merges on that node.
 */
public class MergeMemoryEstimateCollector implements MergeEventListener, ClusterStateListener {
    private static final double DEFAULT_PUBLICATION_MIN_CHANGE_RATIO = 0.1;
    static final TransportVersion AUTOSCALING_MERGE_MEMORY_ESTIMATE_SERVERLESS_VERSION = TransportVersion.fromName(
        "autoscaling_merge_memory_estimate_serverless_version"
    );

    public static final Setting<Double> MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO = Setting.doubleSetting(
        "serverless.autoscaling.memory_metrics.merge_memory_estimate.publication_min_change_ratio",
        DEFAULT_PUBLICATION_MIN_CHANGE_RATIO,
        0.0,
        1.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Supplier<String> localNodeIdSupplier;
    private final Consumer<TransportPublishMergeMemoryEstimate.Request> publisher;
    // We rely on a non-synchronized map and instead ensure that all updates to it are done one by one. This is needed
    // to make sure after each change to the node's currently queued and running merges we produce the correct max shard
    // memory estimate with an increased seq. no.
    private final Map<String, ShardMergeMemoryEstimate> estimates = new HashMap<>();
    private final Object mutex = new Object();
    private ShardMergeMemoryEstimate currentNodeEstimate = ShardMergeMemoryEstimate.NO_MERGES;
    private final Supplier<TransportVersion> minTransportVersionSupplier;
    private volatile double publicationMinChangeRatio = DEFAULT_PUBLICATION_MIN_CHANGE_RATIO;
    private final AtomicLong seqNoGenerator = new AtomicLong();

    /**
     *
     * @param clusterSettings
     * @param minTransportVersionSupplier
     * @param localNodeIdSupplier
     * @param publisher Publications are initiated under lock and therefore the Publisher must be async and not block the caller.
     *                  See e.g. {@link co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimatePublisher}.
     */
    public MergeMemoryEstimateCollector(
        ClusterSettings clusterSettings,
        Supplier<TransportVersion> minTransportVersionSupplier,
        Supplier<String> localNodeIdSupplier,
        Consumer<TransportPublishMergeMemoryEstimate.Request> publisher
    ) {
        this.localNodeIdSupplier = localNodeIdSupplier;
        this.publisher = publisher;
        this.minTransportVersionSupplier = minTransportVersionSupplier;
        clusterSettings.initializeAndWatch(MERGE_MEMORY_ESTIMATE_PUBLICATION_MIN_CHANGE_RATIO, value -> publicationMinChangeRatio = value);
    }

    @Override
    public void onMergeQueued(OnGoingMerge merge, long memoryEstimate) {
        synchronized (mutex) {
            var estimate = new ShardMergeMemoryEstimate(merge.getId(), memoryEstimate);
            var previous = estimates.put(merge.getId(), estimate);
            assert previous == null : "received a mergeQueued event for a merge that is finished already";
            if ((double) estimate.estimateInBytes / currentNodeEstimate.estimateInBytes >= 1 + publicationMinChangeRatio) {
                submitNewEstimateToPublisher(estimate);
            }
        }
    }

    @Override
    public void onMergeCompleted(OnGoingMerge merge) {
        mergeFinished(merge);
    }

    @Override
    public void onMergeAborted(OnGoingMerge merge) {
        mergeFinished(merge);
    }

    private void mergeFinished(OnGoingMerge merge) {
        synchronized (mutex) {
            var previous = estimates.remove(merge.getId());
            assert previous != null : "received a mergeFinished event for an unknown merge";
            if (previous == currentNodeEstimate) {  // last one chosen for publishing is finished
                var currentMax = estimates.values()
                    .stream()
                    .max(ShardMergeMemoryEstimate.COMPARATOR)
                    .orElse(ShardMergeMemoryEstimate.NO_MERGES);
                submitNewEstimateToPublisher(currentMax);
            }
        }
    }

    private void submitNewEstimateToPublisher(ShardMergeMemoryEstimate estimate) {
        assert Thread.holdsLock(mutex);
        if (minTransportVersionSupplier.get().supports(AUTOSCALING_MERGE_MEMORY_ESTIMATE_SERVERLESS_VERSION) == false) {
            return;
        }
        final long seqNo = seqNoGenerator.getAndIncrement();
        final var request = new TransportPublishMergeMemoryEstimate.Request(seqNo, localNodeIdSupplier.get(), estimate);
        publisher.accept(request);
        currentNodeEstimate = estimate;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.nodesDelta().masterNodeChanged()) {
            synchronized (mutex) {
                estimates.values().stream().max(ShardMergeMemoryEstimate.COMPARATOR).ifPresent(this::submitNewEstimateToPublisher);
            }
        }
    }

    public record ShardMergeMemoryEstimate(String mergeId, long estimateInBytes) implements Writeable {
        public static final ShardMergeMemoryEstimate NO_MERGES = new ShardMergeMemoryEstimate("", 0);

        public static final Comparator<ShardMergeMemoryEstimate> COMPARATOR = Comparator.comparingLong(
            ShardMergeMemoryEstimate::estimateInBytes
        ).thenComparing(ShardMergeMemoryEstimate::mergeId);

        public ShardMergeMemoryEstimate(StreamInput in) throws IOException {
            this(in.readString(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(mergeId);
            out.writeVLong(estimateInBytes);
        }
    }

    // Visible for testing
    ShardMergeMemoryEstimate getCurrentNodeEstimate() {
        return currentNodeEstimate;
    }
}
