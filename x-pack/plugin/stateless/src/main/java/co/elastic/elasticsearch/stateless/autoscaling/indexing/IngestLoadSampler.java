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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/**
 * This class takes care of sampling the node indexing load with a given frequency {@code SAMPLING_FREQUENCY_SETTING}
 * and publish the new reading to the elected master periodically (parameterized by {@code MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING}
 * or when the change is significant enough (parameterized by {@code MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING})
 */
public class IngestLoadSampler implements ClusterStateListener {
    public static final Setting<Double> MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING = Setting.doubleSetting(
        "serverless.autoscaling.indexing.sampler.min_sensitivity_ratio_for_publication",
        0.1,
        0,
        0.4,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> SAMPLING_FREQUENCY_SETTING = Setting.timeSetting(
        "serverless.autoscaling.indexing.sampler.frequency",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING = Setting.timeSetting(
        "serverless.autoscaling.indexing.sampler.max_time_between_publications",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    // Minimum transport version required for master to be able to handle the metric publications
    private static final TransportVersion REQUIRED_VERSION = TransportVersion.V_8_500_025;

    private final Logger logger = LogManager.getLogger(IngestLoadSampler.class);
    private final ThreadPool threadPool;
    private final AverageWriteLoadSampler writeLoadSampler;
    private final IngestLoadPublisher ingestionLoadPublisher;
    private final DoubleSupplier currentIndexLoadSupplier;
    private final double minSensitivityRatio;
    private final TimeValue samplingFrequency;
    private final TimeValue maxTimeBetweenPublications;
    private final double numProcessors;
    private final boolean isIndexNode;

    private volatile double ingestionLoad;
    private volatile double latestPublishedIngestionLoad;
    private volatile SamplingTask samplingTask;
    private final AtomicLong lastPublicationRelativeTimeInMillis = new AtomicLong();
    private final AtomicReference<Object> inFlightPublicationTicket = new AtomicReference<>();
    private volatile TransportVersion minTransportVersion = TransportVersion.MINIMUM_COMPATIBLE;
    private volatile String nodeId;

    public IngestLoadSampler(
        ThreadPool threadPool,
        AverageWriteLoadSampler writeLoadSampler,
        IngestLoadPublisher ingestionLoadPublisher,
        DoubleSupplier currentIndexLoadSupplier,
        boolean isIndexNode,
        Settings settings
    ) {
        this(
            threadPool,
            writeLoadSampler,
            ingestionLoadPublisher,
            currentIndexLoadSupplier,
            isIndexNode,
            MIN_SENSITIVITY_RATIO_FOR_PUBLICATION_SETTING.get(settings),
            SAMPLING_FREQUENCY_SETTING.get(settings),
            MAX_TIME_BETWEEN_METRIC_PUBLICATIONS_SETTING.get(settings),
            EsExecutors.nodeProcessors(settings).count()
        );
    }

    IngestLoadSampler(
        ThreadPool threadPool,
        AverageWriteLoadSampler writeLoadSampler,
        IngestLoadPublisher ingestionLoadPublisher,
        DoubleSupplier currentIndexLoadSupplier,
        boolean isIndexNode,
        double minSensitivityRatio,
        TimeValue samplingFrequency,
        TimeValue maxTimeBetweenPublications,
        double numProcessors
    ) {
        if (numProcessors <= 0) {
            throw new IllegalArgumentException("Processors must be positive but was " + numProcessors);
        }

        this.minSensitivityRatio = minSensitivityRatio;
        this.samplingFrequency = samplingFrequency;
        this.maxTimeBetweenPublications = maxTimeBetweenPublications;
        this.numProcessors = numProcessors;
        this.isIndexNode = isIndexNode;

        this.threadPool = threadPool;
        this.writeLoadSampler = writeLoadSampler;
        this.ingestionLoadPublisher = ingestionLoadPublisher;
        this.currentIndexLoadSupplier = currentIndexLoadSupplier;
        // To ensure that the first sample is published right away
        lastPublicationRelativeTimeInMillis.set(threadPool.relativeTimeInMillis() - maxTimeBetweenPublications.getMillis());
    }

    public static IngestLoadSampler create(
        ThreadPool threadPool,
        AverageWriteLoadSampler writeLoadSampler,
        IngestLoadPublisher ingestLoadPublisher,
        DoubleSupplier getIngestionLoad,
        boolean hasIndexRole,
        Settings settings,
        ClusterService clusterService
    ) {
        var loadSampler = new IngestLoadSampler(
            threadPool,
            writeLoadSampler,
            ingestLoadPublisher,
            getIngestionLoad,
            hasIndexRole,
            settings
        );
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                loadSampler.start();
            }

            @Override
            public void beforeStop() {
                loadSampler.stop();
            }
        });
        clusterService.addListener(loadSampler);
        return loadSampler;
    }

    void start() {
        if (isIndexNode) {
            var newSamplingTask = new SamplingTask();
            samplingTask = newSamplingTask;
            newSamplingTask.run();
        }
    }

    void stop() {
        if (isIndexNode) {
            samplingTask = null;
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (isIndexNode == false) {
            return;
        }
        assert nodeId == null || nodeId.equals(event.state().nodes().getLocalNodeId());
        if (nodeId == null) {
            setNodeId(event.state().nodes().getLocalNodeId());
        }
        setMinTransportVersion(event.state().getMinTransportVersion());
        if (event.nodesDelta().masterNodeChanged()) {
            clearInFlightPublicationTicket();
            publishCurrentLoad(nodeId);
        }
    }

    // Visible for testing
    void setMinTransportVersion(TransportVersion minTransportVersion) {
        this.minTransportVersion = minTransportVersion;
    }

    // Visible for testing
    void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    private void sampleIngestionLoad(String nodeId) {
        double previousReading = latestPublishedIngestionLoad;
        double currentReading = currentIndexLoadSupplier.getAsDouble();
        this.ingestionLoad = currentReading;

        var previousRatio = previousReading / numProcessors;
        var currentRatio = currentReading / numProcessors;
        if (currentRatio - previousRatio >= minSensitivityRatio
            || timeSinceLastPublicationInMillis() >= maxTimeBetweenPublications.getMillis()) {
            publishCurrentLoad(nodeId);
        }
    }

    private long timeSinceLastPublicationInMillis() {
        return getRelativeTimeInMillis() - lastPublicationRelativeTimeInMillis.get();
    }

    private void publishCurrentLoad(String nodeId) {
        if (nodeId == null) {
            return;
        }
        if (minTransportVersion.before(REQUIRED_VERSION)) {
            logger.warn("Cannot publish ingest load metric until cluster is: [{}], found: [{}]", REQUIRED_VERSION, minTransportVersion);
            return;
        }
        try {
            var ticket = new Object();
            if (inFlightPublicationTicket.compareAndSet(null, ticket)) {
                final var publishedLoad = ingestionLoad;
                ingestionLoadPublisher.publishIngestionLoad(publishedLoad, nodeId, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        var previousPublicationTime = lastPublicationRelativeTimeInMillis.get();
                        lastPublicationRelativeTimeInMillis.compareAndSet(previousPublicationTime, getRelativeTimeInMillis());
                        latestPublishedIngestionLoad = publishedLoad;
                        inFlightPublicationTicket.compareAndSet(ticket, null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Unable to publish the latest index load", e);
                    }
                });
            }
        } catch (Exception e) {
            assert false : "unexpected exception";
            clearInFlightPublicationTicket();
            logger.warn("Unable to publish latest ingestion load", e);
        }
    }

    private void clearInFlightPublicationTicket() {
        inFlightPublicationTicket.set(null);
    }

    private long getRelativeTimeInMillis() {
        return threadPool.relativeTimeInMillis();
    }

    class SamplingTask implements Runnable {
        @Override
        public void run() {
            if (samplingTask != SamplingTask.this) {
                return;
            }

            try {
                writeLoadSampler.sample();
                sampleIngestionLoad(nodeId);
            } finally {
                threadPool.scheduleUnlessShuttingDown(samplingFrequency, ThreadPool.Names.GENERIC, SamplingTask.this);
            }
        }
    }
}
