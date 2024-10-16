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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
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
public class BlobStoreHealthIndicator extends AbstractLifecycleComponent implements HealthIndicatorService, SchedulerEngine.Listener {

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

    // We extend the timeout and we log a warning on the initial timeout
    // See https://github.com/elastic/elasticsearch-serverless/issues/931
    private static final long WARNING_THRESHOLD_MILLIS = TimeValue.timeValueSeconds(5).millis();
    public static final String CHECK_TIMEOUT = "health.blob_storage.check.timeout";
    public static final Setting<TimeValue> CHECK_TIMEOUT_SETTING = Setting.timeSetting(
        CHECK_TIMEOUT,
        TimeValue.timeValueSeconds(90),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    // Name constant for the job that schedules the blob storage access check
    private static final String BLOB_STORAGE_CHECK_JOB_NAME = "blob_storage_health_check";

    private final Settings settings;
    private final ClusterService clusterService;
    private final StatelessElectionStrategy electionStrategy;
    private final Supplier<Long> currentTimeMillisSupplier;
    private volatile TimeValue pollInterval;
    // Read lease doesn't have a timeout, we implement a timeout by discarding a successful result if it takes longer than the timeout
    private volatile TimeValue timeout;
    private SchedulerEngine.Job scheduledJob;
    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final AtomicBoolean inProgress = new AtomicBoolean(false);
    private volatile Long lastCheckStartedTimeMillis = null;
    private volatile HealthCheckResult lastObservedResult = null;

    public BlobStoreHealthIndicator(
        Settings settings,
        ClusterService clusterService,
        StatelessElectionStrategy electionStrategy,
        Supplier<Long> currentTimeMillisSupplier
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.electionStrategy = electionStrategy;
        this.scheduledJob = null;
        this.pollInterval = POLL_INTERVAL_SETTING.get(settings);
        this.timeout = CHECK_TIMEOUT_SETTING.get(settings);
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
    }

    /**
     * Initializer method to avoid the publication of a self reference in the constructor.
     */
    public BlobStoreHealthIndicator init() {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(POLL_INTERVAL_SETTING, this::updatePollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CHECK_TIMEOUT_SETTING, this::updateTimeout);
        return this;
    }

    @Override
    protected void doStart() {
        logger.info("starting");
        maybeScheduleJob();
    }

    @Override
    protected void doStop() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo ignored) {
        return createHealthIndicatorResult(verbose, lastObservedResult, lastCheckStartedTimeMillis, currentTimeMillisSupplier.get());
    }

    // default visibility for testing purposes
    HealthIndicatorResult createHealthIndicatorResult(
        boolean verbose,
        HealthCheckResult lastObservedResult,
        @Nullable Long lastCheckStartedTimeMillis,
        long currentTimeMillis
    ) {
        // If there is a check in progress that has been running for longer than the timeout we override the local result with a timed out
        // one. We do not override the lastObservedResult field though to avoid races.
        if (hasCheckInProgressTimedOut(lastObservedResult, lastCheckStartedTimeMillis, currentTimeMillis)) {
            lastObservedResult = new HealthCheckResult(
                lastCheckStartedTimeMillis,
                null,
                new ReadBlobStoreCheckTimeout("The blob store health check currently in progress has timed out.")
            );
        }
        return new HealthIndicatorResult(
            NAME,
            getStatus(lastObservedResult, currentTimeMillis),
            getSymptom(lastObservedResult, currentTimeMillis),
            getDetails(verbose, lastObservedResult, currentTimeMillis),
            getImpacts(lastObservedResult),
            getDiagnoses(lastObservedResult, currentTimeMillis)
        );
    }

    private HealthStatus getStatus(HealthCheckResult result, long currentTimeMillis) {
        if (result == null) {
            // The cluster just started and there hasn't been a check, we start with a green state.
            return HealthStatus.GREEN;
        }
        // If the latest result successful but it is too stale we switch it to YELLOW to indicate that our confidence on it has decreased.
        if (isResultStale(result, currentTimeMillis)) {
            return HealthStatus.YELLOW;
        }
        return result.isSuccessful() ? HealthStatus.GREEN : HealthStatus.RED;
    }

    private String getSymptom(HealthCheckResult result, long currentTimeMillis) {
        if (result == null) {
            return "The cluster is initialising, the first health check hasn't been completed yet.";
        }
        // If the latest result successful but it is too stale we switch it to YELLOW to indicate that our confidence on it has decreased.
        if (isResultStale(result, currentTimeMillis)) {
            return "It is uncertain that the cluster can access the blob store, last successful check started "
                + TimeValue.timeValueMillis(currentTimeMillis - result.startedAtMillis()).toHumanReadableString(2)
                + " ago.";
        }
        return result.isSuccessful() ? "The cluster can access the blob store." : "The cluster failed to access the blob store.";
    }

    private HealthIndicatorDetails getDetails(boolean verbose, HealthCheckResult result, long currentTimeMillis) {
        if (verbose == false || (result == null && lastCheckStartedTimeMillis == null)) {
            return HealthIndicatorDetails.EMPTY;
        }
        return ((builder, params) -> {
            builder.startObject();
            durationMillisField(builder, "time_since_last_check_started", currentTimeMillis - lastCheckStartedTimeMillis);
            if (result != null) {
                if (result.finishedAtMillis() != null) {
                    durationMillisField(builder, "time_since_last_update", currentTimeMillis - result.finishedAtMillis());
                    durationMillisField(builder, "last_check_duration", result.finishedAtMillis() - result.startedAtMillis());
                }
                if (result.isSuccessful() == false) {
                    builder.field("error_message", result.error().getMessage());
                }
            }
            return builder.endObject();
        });
    }

    private static void durationMillisField(XContentBuilder builder, String fieldNamePrefix, long durationMillis) throws IOException {
        if (durationMillis < 0) {
            builder.field(fieldNamePrefix + "_millis", durationMillis);
        } else {
            builder.humanReadableField(fieldNamePrefix + "_millis", fieldNamePrefix, TimeValue.timeValueMillis(durationMillis));
        }
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

    private List<Diagnosis> getDiagnoses(HealthCheckResult result, long currentTimeMillis) {
        if (isResultStale(result, currentTimeMillis)) {
            return List.of(
                new Diagnosis(
                    new Diagnosis.Definition(
                        NAME,
                        "stale_status",
                        "There have been no health checks for "
                            + TimeValue.timeValueMillis(currentTimeMillis - result.startedAtMillis()).toHumanReadableString(2),
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
    private boolean isResultStale(HealthCheckResult result, long currentTimeMillis) {
        return result != null
            && result.isSuccessful()
            && currentTimeMillis - result.finishedAtMillis() > STALENESS_FACTOR * pollInterval.millis();
    }

    // A check is in progress if the start time of the check is newer than the start time of the last observed result. We do not
    // use the inProgress flag to avoid race that would appear like an older start time is still in progress.
    private boolean hasCheckInProgressTimedOut(HealthCheckResult lastResult, @Nullable Long startedAtMillis, long currentTimeMillis) {
        boolean checkInProgress = startedAtMillis != null && (lastResult == null || lastResult.startedAtMillis() < startedAtMillis);
        return checkInProgress && currentTimeMillis - startedAtMillis > timeout.millis();
    }

    /**
     * Captures the result of a health check.
     * @param startedAtMillis the time the health check started
     * @param finishedAtMillis the time the health check finished, null means that it hasn't finished yet, but it is considered a timeout
     * @param error the error if the blob store was not accessible or null otherwise
     */
    record HealthCheckResult(long startedAtMillis, @Nullable Long finishedAtMillis, @Nullable Exception error) {

        boolean isSuccessful() {
            return error == null;
        }

        static boolean isSameError(HealthCheckResult r1, HealthCheckResult r2) {
            assert r1.isSuccessful() == false && r2.isSuccessful() == false;
            return Objects.equals(r1.error().getClass(), r2.error().getClass());
        }
    }

    @Override
    public boolean isPreflight() {
        return true;
    }

    @Override
    protected void doClose() {
        logger.info("closing");
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
        if (lifecycle.started() == false) {
            return;
        }
        if (inProgress.compareAndSet(false, true)) {
            lastCheckStartedTimeMillis = currentTimeMillisSupplier.get();
            electionStrategy.readLease(ActionListener.releaseAfter(new ActionListener<>() {
                @Override
                public void onResponse(Optional<StatelessElectionStrategy.Lease> lease) {
                    long currentTimeMillis = currentTimeMillisSupplier.get();
                    long durationMillis = currentTimeMillis - lastCheckStartedTimeMillis;
                    // See https://github.com/elastic/elasticsearch-serverless/issues/931
                    if (durationMillis > WARNING_THRESHOLD_MILLIS) {
                        logger.warn(
                            "Blob store read duration is higher than the warn threshold, read took {}.",
                            TimeValue.timeValueMillis(durationMillis).toHumanReadableString(2)
                        );
                    }
                    setNewResult(
                        durationMillis <= timeout.millis()
                            ? new HealthCheckResult(lastCheckStartedTimeMillis, currentTimeMillis, null)
                            : new HealthCheckResult(
                                lastCheckStartedTimeMillis,
                                currentTimeMillis,
                                new ReadBlobStoreCheckTimeout(
                                    "Reading from the blob store took "
                                        + TimeValue.timeValueMillis(durationMillis).toHumanReadableString(2)
                                        + " which is longer than the timeout "
                                        + timeout.toHumanReadableString(2)
                                )
                            )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    setNewResult(new HealthCheckResult(lastCheckStartedTimeMillis, currentTimeMillisSupplier.get(), e));
                }
            }, () -> inProgress.set(false)));
        }
    }

    private void setNewResult(HealthCheckResult result) {
        if (lastObservedResult != null) {
            if (result.isSuccessful() && lastObservedResult.isSuccessful() == false) {
                logger.info("Blob store is accessible");
            } else if (result.isSuccessful() == false
                && (lastObservedResult.isSuccessful() || HealthCheckResult.isSameError(result, lastObservedResult) == false)) {
                    logger.warn(() -> "Blob store is not accessible: " + result.error().getMessage(), result.error());
                }
        }
        lastObservedResult = result;
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
