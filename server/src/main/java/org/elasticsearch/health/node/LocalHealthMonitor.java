/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.health.HealthFeatures;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.action.HealthNodeNotDiscoveredException;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskExecutor;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;

/**
 * This class monitors the local health of the node, such as the load and any errors that can be specific to a node
 * (as opposed to errors that are cluster-wide). It informs the health node about the local health upon change, or
 * when a new node is detected, or when the master node changed.
 */
public class LocalHealthMonitor implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(LocalHealthMonitor.class);

    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "health.reporting.local.monitor.interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Client client;
    private final FeatureService featureService;

    private volatile TimeValue monitorInterval;
    private volatile boolean enabled;

    // Signals that all the prerequisites have been fulfilled and the monitoring can be started.
    private volatile boolean prerequisitesFulfilled;

    // List of health trackers to be executed in each monitoring cycle.
    private final List<HealthTracker<?>> healthTrackers;
    // Keeps the last seen health node. We use this variable to ensure that there wasn't a health node
    // change between the time we send an update until the time we record the last health state that was successfully reported.
    private final AtomicReference<String> lastSeenHealthNode = new AtomicReference<>();
    // Using a volatile reference to ensure that there is a single instance of monitoring running at all times.
    // No need for extra synchronization because all the writes are executed on the cluster applier thread.
    private volatile Monitoring monitoring;
    // This variable keeps track of whether there's an in-flight request. We keep this variable here rather than the Monitoring class,
    // as we'll create new instances of that class when we're (re)starting this local health monitoring process.
    // This variable allows us to ensure that there's always, at most, 1 request in-flight, at any given moment.
    private final AtomicReference<Boolean> inFlightRequest = new AtomicReference<>(false);

    private LocalHealthMonitor(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        FeatureService featureService,
        List<HealthTracker<?>> healthTrackers
    ) {
        this.threadPool = threadPool;
        this.monitorInterval = POLL_INTERVAL_SETTING.get(settings);
        this.enabled = HealthNodeTaskExecutor.ENABLED_SETTING.get(settings);
        this.clusterService = clusterService;
        this.client = client;
        this.featureService = featureService;
        this.healthTrackers = healthTrackers;
    }

    public static LocalHealthMonitor create(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        FeatureService featureService,
        List<HealthTracker<?>> healthTrackers
    ) {
        LocalHealthMonitor localHealthMonitor = new LocalHealthMonitor(
            settings,
            clusterService,
            threadPool,
            client,
            featureService,
            healthTrackers
        );
        localHealthMonitor.registerListeners();
        return localHealthMonitor;
    }

    private void registerListeners() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::setMonitorInterval);
        clusterSettings.addSettingsUpdateConsumer(HealthNodeTaskExecutor.ENABLED_SETTING, this::setEnabled);
        clusterService.addListener(this);
    }

    // When the monitoring interval changes, we restart the health monitoring with the new interval.
    void setMonitorInterval(TimeValue monitorInterval) {
        this.monitorInterval = monitorInterval;
        stopMonitoring();
        startMonitoringIfNecessary();
    }

    // When the health node is enabled we try to start monitoring if it is not
    // already running, no need to restart it since there was no configuration
    // change. When the health node is disabled we stop monitoring.
    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            startMonitoringIfNecessary();
        } else {
            stopMonitoring();
        }
    }

    private void stopMonitoring() {
        // If there is an existing schedule, cancel it
        Scheduler.Cancellable currentMonitoring = monitoring;
        if (currentMonitoring != null) {
            currentMonitoring.cancel();
        }
    }

    private void startMonitoringIfNecessary() {
        if (prerequisitesFulfilled && enabled) {
            if (isMonitorRunning() == false) {
                monitoring = Monitoring.start(
                    monitorInterval,
                    threadPool,
                    lastSeenHealthNode,
                    healthTrackers,
                    clusterService,
                    client,
                    inFlightRequest
                );
                logger.debug("Local health monitoring started {}", monitoring);
            } else {
                logger.trace("Local health monitoring already started {}, skipping", monitoring);
            }
        }
    }

    private boolean isMonitorRunning() {
        Scheduler.Cancellable scheduled = this.monitoring;
        return scheduled != null && scheduled.isCancelled() == false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(event.state());
        DiscoveryNode currentMasterNode = event.state().nodes().getMasterNode();
        boolean healthNodeChanged = hasHealthNodeChanged(currentHealthNode, event);
        boolean masterNodeChanged = hasMasterNodeChanged(currentMasterNode, event);
        if (healthNodeChanged || masterNodeChanged) {
            // On health node or on master node changes, the health node might be reset so the reported
            // health info gets reset to null, to ensure it will be resent.
            lastSeenHealthNode.set(currentHealthNode == null ? null : currentHealthNode.getId());
            // Reset the reference of each HealthTracker.
            healthTrackers.forEach(HealthTracker::reset);
            if (logger.isDebugEnabled()) {
                String reason;
                if (healthNodeChanged && masterNodeChanged) {
                    reason = "the master node and the health node";
                } else if (healthNodeChanged) {
                    reason = "the health node";
                } else {
                    reason = "the master node";
                }
                logger.debug(
                    "Resetting the health monitoring because {} changed, current health node is {}.",
                    reason,
                    currentHealthNode == null ? null : format("[%s][%s]", currentHealthNode.getName(), currentHealthNode.getId())
                );
            }
        }
        prerequisitesFulfilled = event.state().clusterRecovered()
            && featureService.clusterHasFeature(event.state(), HealthFeatures.SUPPORTS_HEALTH)
            && HealthMetadata.getFromClusterState(event.state()) != null
            && currentHealthNode != null
            && currentMasterNode != null;
        if (prerequisitesFulfilled == false || healthNodeChanged || masterNodeChanged) {
            stopMonitoring();
        }
        if (prerequisitesFulfilled) {
            startMonitoringIfNecessary();
        }
    }

    private static boolean hasMasterNodeChanged(DiscoveryNode currentMasterNode, ClusterChangedEvent event) {
        DiscoveryNode previousMasterNode = event.previousState().nodes().getMasterNode();
        if (currentMasterNode == null || previousMasterNode == null) {
            return currentMasterNode != previousMasterNode;
        }
        return previousMasterNode.getEphemeralId().equals(currentMasterNode.getEphemeralId()) == false;
    }

    // We compare the current health node against both the last seen health node from this node and the
    // health node reported in the previous cluster state to be safe that we do not miss any change due to
    // a flaky state.
    private boolean hasHealthNodeChanged(DiscoveryNode currentHealthNode, ClusterChangedEvent event) {
        DiscoveryNode previousHealthNode = HealthNode.findHealthNode(event.previousState());
        return Objects.equals(lastSeenHealthNode.get(), currentHealthNode == null ? null : currentHealthNode.getId()) == false
            || Objects.equals(previousHealthNode, currentHealthNode) == false;
    }

    /**
     * This class is responsible for running the health monitoring. It evaluates and checks the health info of this node
     * in the configured intervals. The first run happens upon initialization. If there is an exception, it will log it
     * and continue to schedule the next run.
     * Usually, there will only be one instance of this class alive. However, when we're restarting
     * the monitoring process (e.g. due to a health node change, see {@link LocalHealthMonitor#clusterChanged}), there will likely (shortly)
     * be two instances alive at the same time. To avoid any concurrency issues, we're ensuring that there's always only one in-flight
     * request and if a {@link Monitoring} instance is cancelled while a request is in-flight, we'll prevent it from updating the state
     * of the {@link HealthTracker}s (and it'll be up to the next/new {@link Monitoring} instance to send a new request and update the
     * {@link HealthTracker}s' state).
     */
    static class Monitoring implements Runnable, Scheduler.Cancellable {

        private final TimeValue interval;
        private final Executor executor;
        private final Scheduler scheduler;
        private final ClusterService clusterService;
        private final Client client;

        private final AtomicReference<String> lastSeenHealthNode;
        private final List<HealthTracker<?>> healthTrackers;

        private final AtomicReference<Boolean> inFlightRequest;
        private volatile boolean cancelled = false;
        private volatile Scheduler.ScheduledCancellable scheduledRun;

        private Monitoring(
            TimeValue interval,
            Scheduler scheduler,
            Executor executor,
            AtomicReference<String> lastSeenHealthNode,
            List<HealthTracker<?>> healthTrackers,
            ClusterService clusterService,
            Client client,
            AtomicReference<Boolean> inFlightRequest
        ) {
            this.interval = interval;
            this.executor = executor;
            this.scheduler = scheduler;
            this.lastSeenHealthNode = lastSeenHealthNode;
            this.clusterService = clusterService;
            this.healthTrackers = healthTrackers;
            this.client = client;
            this.inFlightRequest = inFlightRequest;
        }

        /**
         * Creates a monitoring instance and starts the schedules the first run.
         */
        static Monitoring start(
            TimeValue interval,
            ThreadPool threadPool,
            AtomicReference<String> lastSeenHealthNode,
            List<HealthTracker<?>> healthTrackers,
            ClusterService clusterService,
            Client client,
            AtomicReference<Boolean> inFlightRequest
        ) {
            Monitoring monitoring = new Monitoring(
                interval,
                threadPool,
                threadPool.executor(ThreadPool.Names.MANAGEMENT),
                lastSeenHealthNode,
                healthTrackers,
                clusterService,
                client,
                inFlightRequest
            );
            monitoring.scheduledRun = threadPool.schedule(monitoring, TimeValue.ZERO, monitoring.executor);
            return monitoring;
        }

        /**
         * Attempts to cancel monitoring. This method has no effect if
         * the monitoring is already cancelled. If the {@code scheduledRun}
         * has not started when {@code cancel} is called, this run should
         * never run. If the {@code scheduledRun} is already running, then
         * it will not be interrupted but the next run will not be scheduled.
         *
         * @return false, if the {@code scheduledRun} was already cancelled; true
         * otherwise.
         */
        @Override
        public boolean cancel() {
            if (cancelled) {
                // already cancelled
                return false;
            }
            cancelled = true;
            scheduledRun.cancel();
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        /**
         * This method evaluates the health info of this node and if there is a change it sends an update request to the health node.
         */
        @Override
        public void run() {
            if (cancelled) {
                return;
            }
            boolean nextRunScheduled = false;
            Runnable scheduleNextRun = new RunOnce(this::scheduleNextRunIfNecessary);
            // Before we do anything, we're first going to make sure there is no in-flight request at this moment.
            // If that's the case, we'll acquire the "lock", which prevents any other thread/instance from sending any requests.
            if (inFlightRequest.compareAndSet(false, true) == false) {
                logger.debug("Not allowed to send health info update request due to in-flight request, will try again.");
                scheduleNextRun.run();
                return;
            }
            try {
                List<HealthTracker.HealthProgress<?>> healthProgresses = getHealthProgresses();
                if (healthProgresses.isEmpty()) {
                    // Next run will still be scheduled in the `finally` block.
                    return;
                }
                // Create builder and add the current value of each (changed) health tracker to the request.
                var builder = new UpdateHealthInfoCacheAction.Request.Builder().nodeId(clusterService.localNode().getId());
                healthProgresses.forEach(changedHealthInfo -> changedHealthInfo.updateRequestBuilder(builder));

                var healthNodeId = lastSeenHealthNode.get();
                var listener = ActionListener.<AcknowledgedResponse>wrap(response -> {
                    // Don't update the latest health info if the health node has changed while this request was being processed.
                    if (Objects.equals(healthNodeId, lastSeenHealthNode.get()) == false) {
                        return;
                    }
                    // Only record health progress if this monitoring instance hasn't been cancelled in the meantime.
                    // This avoids any unwanted writes to the HealthTrackers' states after a new monitoring instance has possibly
                    // already started.
                    if (cancelled == false) {
                        healthProgresses.forEach(HealthTracker.HealthProgress::recordProgressIfRelevant);
                    }
                    inFlightRequest.set(false);
                }, e -> {
                    if (e.getCause() instanceof NodeNotConnectedException || e.getCause() instanceof HealthNodeNotDiscoveredException) {
                        logger.debug("Failed to connect to the health node [{}], will try again.", e.getCause().getMessage());
                    } else {
                        logger.debug(() -> format("Failed to send health info to health node, will try again."), e);
                    }
                    inFlightRequest.set(false);
                });
                client.execute(UpdateHealthInfoCacheAction.INSTANCE, builder.build(), ActionListener.runAfter(listener, scheduleNextRun));
                nextRunScheduled = true;
            } catch (Exception e) {
                logger.warn(() -> format("Failed to run scheduled health monitoring on thread pool [%s]", executor), e);
                // Make sure to release the "lock" in case something goes wrong, so we can try again in the next iteration.
                inFlightRequest.set(false);
            } finally {
                // If the next run isn't scheduled because for example the health info hasn't changed, we schedule it here.
                if (nextRunScheduled == false) {
                    scheduleNextRun.run();
                }
            }
        }

        /**
         * Retrieve the current health of each tracker and return a list of the ones that have changed.
         *
         * @return a list of changed health info's.
         */
        private List<HealthTracker.HealthProgress<?>> getHealthProgresses() {
            var healthMetadata = HealthMetadata.getFromClusterState(clusterService.state());
            // Don't try to run the health trackers if the HealthMetadata is not available.
            if (healthMetadata == null) {
                return List.of();
            }

            return healthTrackers.stream().<HealthTracker.HealthProgress<?>>map(HealthTracker::trackHealth)
                // Only return changed values.
                .filter(HealthTracker.HealthProgress::hasChanged)
                .toList();
        }

        private void scheduleNextRunIfNecessary() {
            if (cancelled) {
                return;
            }
            try {
                scheduledRun = scheduler.schedule(this, interval, executor);
            } catch (final EsRejectedExecutionException e) {
                logger.debug(() -> format("Scheduled health monitoring was rejected on thread pool [%s]", executor), e);
            }
        }

        @Override
        public String toString() {
            return "Monitoring{interval=" + interval + ", cancelled=" + cancelled + "}";
        }
    }
}
