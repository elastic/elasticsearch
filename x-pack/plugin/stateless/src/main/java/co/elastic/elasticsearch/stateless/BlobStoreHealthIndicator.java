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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.node.HealthInfo;

import java.io.Closeable;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This health indicator checks at regular intervals if it can read the lease from the blob store to determine
 * if the cluster can access the blob store or not. The health status is based on the last observed health check result:
 * - GREEN, if there were no errors and the result is recent enough or if the service is initialising
 * - YELLOW, if the last observed result was GREEN, but it was observed longer than X times the poll interval
 * - RED, if there was an error during the last check or if the current check has exceeded the timeout.
 */
public class BlobStoreHealthIndicator implements HealthIndicatorService, Closeable, SchedulerEngine.Listener {

    private static final Logger logger = LogManager.getLogger(BlobStoreHealthIndicator.class);

    public static final String NAME = "blob_store";

    public static final String POLL_INTERVAL = "health.blob_storage.poll_interval";
    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        POLL_INTERVAL,
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    // We apply this factor to the poll interval to determine when is a health check stale
    private static final int STALENESS_FACTOR = 2;
    public static final String CHECK_TIMEOUT = "health.blob_storage.check.timeout";
    public static final Setting<TimeValue> CHECK_TIMEOUT_SETTING = Setting.timeSetting(
        CHECK_TIMEOUT,
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // Name constant for the job that schedules the blob storage access check
    private static final String BLOB_STORAGE_CHECK_JOB_NAME = "blob_storage_health_check";

    private final Settings settings;
    private final ClusterService clusterService;
    private final StatelessElectionStrategy electionStrategy;
    private final Supplier<Long> timeSupplier;
    private volatile TimeValue pollInterval;
    // Read lease doesn't have a timeout, we implement a timeout by discarding a successful result if it takes longer than the timeout
    private volatile TimeValue timeout;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private volatile Long lastCheckStarted = null;
    private volatile HealthCheckResult lastObservedResult = null;

    public BlobStoreHealthIndicator(
        Settings settings,
        ClusterService clusterService,
        StatelessElectionStrategy electionStrategy,
        Supplier<Long> timeSupplier
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.electionStrategy = electionStrategy;
        this.scheduledJob = null;
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.timeout = CHECK_TIMEOUT_SETTING.get(settings);
        this.timeSupplier = timeSupplier;
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public BlobStoreHealthIndicator init() {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CHECK_TIMEOUT_SETTING, this::updateTimeout);
        maybeScheduleJob();
        return this;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo ignored) {
        return createHealthIndicatorResult(verbose, lastObservedResult, lastCheckStarted, timeSupplier.get());
    }

    // default visibility for testing purposes
    HealthIndicatorResult createHealthIndicatorResult(boolean verbose, HealthCheckResult currentResult, Long lastCheckStarted, long now) {
        // If there is a check in progress that has been running for longer than the timeout we override the local result with a timed out
        // one. We do not override the lastObservedResult field though to avoid races.
        if (hasCheckInProgressTimedOut(currentResult, lastCheckStarted, now)) {
            currentResult = new HealthCheckResult(
                lastCheckStarted,
                null,
                new ReadBlobStoreCheckTimeout("The blob store health check currently in progress has timed out.")
            );
        }
        return new HealthIndicatorResult(
            NAME,
            getStatus(currentResult, now),
            getSymptom(currentResult, now),
            getDetails(verbose, currentResult, now),
            getImpacts(currentResult),
            getDiagnoses(currentResult, now)
        );
    }

    private HealthStatus getStatus(HealthCheckResult result, long now) {
        if (result == null) {
            // The cluster just started and there hasn't been a check, we start with a green state.
            return HealthStatus.GREEN;
        }
        // If the latest result successful but it is too stale we switch it to YELLOW to indicate that our confidence on it has decreased.
        if (isResultStale(result, now)) {
            return HealthStatus.YELLOW;
        }
        return result.isSuccessful() ? HealthStatus.GREEN : HealthStatus.RED;
    }

    private String getSymptom(HealthCheckResult result, long now) {
        if (result == null) {
            return "The cluster is initialising, the first health check hasn't been completed yet.";
        }
        // If the latest result successful but it is too stale we switch it to YELLOW to indicate that our confidence on it has decreased.
        if (isResultStale(result, now)) {
            return "It is uncertain that the cluster can access the blob store, last successful check started "
                + TimeValue.timeValueMillis(now - result.startedAt()).toHumanReadableString(2)
                + " ago.";
        }
        return result.isSuccessful() ? "The cluster can access the blob store." : "The cluster failed to access the blob store.";
    }

    private HealthIndicatorDetails getDetails(boolean verbose, HealthCheckResult result, long now) {
        if (verbose == false || (result == null && lastCheckStarted == null)) {
            return HealthIndicatorDetails.EMPTY;
        }
        return ((builder, params) -> {
            builder.startObject();
            builder.timeField("time_since_last_check_started_millis", "time_since_last_check_started", now - lastCheckStarted);
            if (result != null) {
                if (result.finishedAt() != null) {
                    builder.timeField("time_since_last_update_millis", "time_since_last_update", now - result.finishedAt());
                    builder.timeField("last_check_duration_millis", "last_check_duration", result.finishedAt() - result.startedAt());
                }
                if (result.isSuccessful() == false) {
                    builder.field("error_message", result.error().getMessage());
                }
            }
            return builder.endObject();
        });
    }

    private List<HealthIndicatorImpact> getImpacts(HealthCheckResult result) {
        if (result == null || result.isSuccessful()) {
            return List.of();
        }
        return List.of(
            new HealthIndicatorImpact(
                NAME,
                "data_unavailable",
                1,
                "The cluster cannot reach any data and metadata, consequently it cannot search, insert, update or backup user data"
                    + " and all deployment management tasks such as master election are at risk",
                List.of(ImpactArea.INGEST, ImpactArea.SEARCH, ImpactArea.DEPLOYMENT_MANAGEMENT, ImpactArea.BACKUP)
            )
        );
    }

    private List<Diagnosis> getDiagnoses(HealthCheckResult result, long now) {
        if (isResultStale(result, now)) {
            return List.of(
                new Diagnosis(
                    new Diagnosis.Definition(
                        NAME,
                        "stale_status",
                        "There have been no health checks for "
                            + TimeValue.timeValueMillis(now - result.startedAt()).toHumanReadableString(2),
                        "Please investigate the logs to see if the health check job is running.",
                        ""
                    ),
                    List.of()
                )
            );
        }
        if (result == null || result.isSuccessful()) {
            return List.of();
        }
        return List.of(
            new Diagnosis(
                new Diagnosis.Definition(NAME, "blob_store_inaccessible", result.error().getMessage(), "Contact control plane SRE", ""),
                List.of()
            )
        );
    }

    // A stale result us a "success" result that has been observed longer than X times the poll interval
    private boolean isResultStale(HealthCheckResult result, long now) {
        return result != null && result.isSuccessful() && now - result.finishedAt() > STALENESS_FACTOR * pollInterval.millis();
    }

    // A check is in progress if the start time of the check is newer than the start time of the last observed result. We do not
    // use the inProgress flag to avoid race that would appear like an older start time is still in progress.
    private boolean hasCheckInProgressTimedOut(HealthCheckResult lastResult, Long startedAt, long now) {
        boolean checkInProgress = startedAt != null && (lastResult == null || lastResult.startedAt() < startedAt);
        return checkInProgress && now - startedAt > timeout.millis();
    }

    /**
     * Captures the result of a health check.
     * @param startedAt the time the health check started
     * @param finishedAt the time the health check finished, null means that it hasn't finished yet, but it is considered a timeout
     * @param error the error if the blob store was not accessible or null otherwise
     */
    record HealthCheckResult(long startedAt, @Nullable Long finishedAt, @Nullable Exception error) {

        boolean isSuccessful() {
            return error == null;
        }
    }

    @Override
    public boolean isPreflight() {
        return true;
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(BLOB_STORAGE_CHECK_JOB_NAME)) {
            logger.trace(
                "Blob storage health check job triggered: {}, {}, {}",
                event.getJobName(),
                event.getScheduledTime(),
                event.getTriggeredTime()
            );
            runCheck();
        }
    }

    // default visibility for testing purposes
    void runCheck() {
        if (inProgress.compareAndSet(false, true)) {
            lastCheckStarted = timeSupplier.get();
            electionStrategy.readLease(ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(Optional<StatelessElectionStrategy.Lease> lease) {
                    long now = timeSupplier.get();
                    long duration = now - lastCheckStarted;
                    lastObservedResult = duration <= timeout.millis()
                        ? new HealthCheckResult(lastCheckStarted, now, null)
                        : new HealthCheckResult(
                            lastCheckStarted,
                            now,
                            new ReadBlobStoreCheckTimeout(
                                "Reading from the blob store took "
                                    + TimeValue.timeValueMillis(duration).toHumanReadableString(2)
                                    + " which is longer than the timeout "
                                    + timeout.toHumanReadableString(2)
                            )
                        );
                    if (lastObservedResult.isSuccessful()) {
                        logger.trace("Blob store is accessible");
                    } else {
                        logger.trace("Blob store check timed out.");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    lastObservedResult = new HealthCheckResult(lastCheckStarted, timeSupplier.get(), e);
                    logger.trace("Blob store is not accessible.");
                }
            }, () -> inProgress.set(false)));
        }
    }

    // for testing purposes
    boolean getInProgress() {
        return inProgress.get();
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
    }

    private void updateTimeout(TimeValue newTimeout) {
        this.timeout = newTimeout;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    private boolean isClusterServiceStoppedOrClosed() {
        final Lifecycle.State state = clusterService.lifecycleState();
        return state == Lifecycle.State.STOPPED || state == Lifecycle.State.CLOSED;
    }

    private void maybeScheduleJob() {
        // don't schedule the job if the node is shutting down
        if (isClusterServiceStoppedOrClosed()) {
            logger.trace(
                "Skipping scheduling a blob store health check job due to the cluster lifecycle state being: [{}] ",
                clusterService.lifecycleState()
            );
            return;
        }

        if (scheduler.get() == null) {
            scheduler.set(new SchedulerEngine(settings, Clock.systemUTC()));
            scheduler.get().register(this);
        }

        assert scheduler.get() != null : "scheduler should be available";
        scheduledJob = new SchedulerEngine.Job(BLOB_STORAGE_CHECK_JOB_NAME, new TimeValueSchedule(pollInterval));
        scheduler.get().add(scheduledJob);
    }

    static class ReadBlobStoreCheckTimeout extends Exception {
        ReadBlobStoreCheckTimeout(String message) {
            super(message);
        }
    }
}
