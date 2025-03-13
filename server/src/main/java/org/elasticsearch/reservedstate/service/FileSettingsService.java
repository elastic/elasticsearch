/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.file.MasterNodeFileWatchingService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ImpactArea.DEPLOYMENT_MANAGEMENT;
import static org.elasticsearch.reservedstate.service.ReservedStateVersionCheck.HIGHER_OR_SAME_VERSION;
import static org.elasticsearch.reservedstate.service.ReservedStateVersionCheck.HIGHER_VERSION_ONLY;
import static org.elasticsearch.xcontent.XContentType.JSON;

/**
 * File based settings applier service which watches an 'operator` directory inside the config directory.
 * <p>
 * The service expects that the operator directory will contain a single JSON file with all the settings that
 * need to be applied to the cluster state. The name of the file is fixed to be settings.json. The operator
 * directory name can be configured by setting the 'path.config.operator_directory' in the node properties.
 * <p>
 * The {@link FileSettingsService} is active always, but enabled only on the current master node. We register
 * the service as a listener to cluster state changes, so that we can enable the file watcher thread when this
 * node becomes a master node.
 */
public class FileSettingsService extends MasterNodeFileWatchingService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(FileSettingsService.class);

    public static final String SETTINGS_FILE_NAME = "settings.json";
    public static final String NAMESPACE = "file_settings";
    public static final String OPERATOR_DIRECTORY = "operator";

    private final Path watchedFile;
    private final ReservedClusterStateService stateService;
    private final FileSettingsHealthIndicatorService healthIndicatorService;

    /**
     * Constructs the {@link FileSettingsService}
     *
     * @param clusterService so we can register ourselves as a cluster state change listener
     * @param stateService an instance of the immutable cluster state controller, so we can perform the cluster state changes
     * @param environment we need the environment to pull the location of the config and operator directories
     * @param healthIndicatorService tracks the success or failure of file-based settings
     */
    @SuppressWarnings("this-escape")
    public FileSettingsService(
        ClusterService clusterService,
        ReservedClusterStateService stateService,
        Environment environment,
        FileSettingsHealthIndicatorService healthIndicatorService
    ) {
        super(clusterService, environment.configDir().toAbsolutePath().resolve(OPERATOR_DIRECTORY));
        this.watchedFile = watchedFileDir().resolve(SETTINGS_FILE_NAME);
        this.stateService = stateService;
        this.healthIndicatorService = healthIndicatorService;
    }

    protected Logger logger() {
        return logger;
    }

    public Path watchedFile() {
        return watchedFile;
    }

    public FileSettingsHealthIndicatorService healthIndicatorService() {
        return healthIndicatorService;
    }

    /**
     * Used by snapshot restore service {@link org.elasticsearch.snapshots.RestoreService} to prepare the reserved
     * state of the snapshot for the current cluster.
     * <p>
     * If the current cluster where we are restoring the snapshot into has any operator file based settings, we'll
     * reset the reserved state version to 0.
     * <p>
     * If there's no file based settings file in this cluster, we'll remove all state reservations for
     * file based settings from the cluster state.
     * @param clusterState the cluster state before snapshot restore
     * @param mdBuilder the current metadata builder for the new cluster state
     */
    public void handleSnapshotRestore(ClusterState clusterState, Metadata.Builder mdBuilder) {
        assert clusterState.nodes().isLocalNodeElectedMaster();

        ReservedStateMetadata fileSettingsMetadata = clusterState.metadata().reservedStateMetadata().get(NAMESPACE);

        // When we restore from a snapshot we remove the reserved cluster state for file settings,
        // since we don't know the current operator configuration, e.g. file settings could be disabled
        // on the target cluster. If file settings exist and the cluster state has lost it's reserved
        // state for the "file_settings" namespace, we touch our file settings file to cause it to re-process the file.
        if (watching() && Files.exists(watchedFile)) {
            if (fileSettingsMetadata != null) {
                ReservedStateMetadata withResetVersion = new ReservedStateMetadata.Builder(fileSettingsMetadata).version(0L).build();
                mdBuilder.put(withResetVersion);
            }
        } else if (fileSettingsMetadata != null) {
            mdBuilder.removeReservedState(fileSettingsMetadata);
        }
    }

    @Override
    protected void doStart() {
        healthIndicatorService.startOccurred();
        super.doStart();
    }

    @Override
    protected void doStop() {
        super.doStop();
        healthIndicatorService.stopOccurred();
    }

    /**
     * If the file settings metadata version is set to zero, then we have restored from
     * a snapshot and must reprocess the file.
     * @param clusterState State of the cluster
     * @return true if file settings metadata version is exactly 0, false otherwise.
     */
    @Override
    protected boolean shouldRefreshFileState(ClusterState clusterState) {
        // We check if the version was reset to 0, and force an update if a file exists. This can happen in situations
        // like snapshot restores.
        ReservedStateMetadata fileSettingsMetadata = clusterState.metadata().reservedStateMetadata().get(NAMESPACE);
        return fileSettingsMetadata != null && fileSettingsMetadata.version().equals(ReservedStateMetadata.RESTORED_VERSION);
    }

    /**
     * Read settings and pass them to {@link ReservedClusterStateService} for application
     *
     * @throws IOException if there is an error reading the file itself
     * @throws ExecutionException if there is an issue while applying the changes from the file
     * @throws InterruptedException if the file processing is interrupted by another thread.
     */
    @Override
    protected final void processFileChanges(Path file) throws ExecutionException, InterruptedException, IOException {
        processFile(file, false);
    }

    /**
     * Read settings and pass them to {@link ReservedClusterStateService} for application.
     * Settings will be reprocessed even if the cluster-state version equals that found in the settings file.
     */
    @Override
    protected final void processFileOnServiceStart(Path file) throws IOException, ExecutionException, InterruptedException {
        processFile(file, true);
    }

    protected void processFile(Path file, boolean startup) throws IOException, ExecutionException, InterruptedException {
        if (watchedFile.equals(file) == false) {
            logger().debug("Received notification for unknown file {}", file);
        } else {
            logger().info("processing path [{}] for [{}]{}", watchedFile, NAMESPACE, startup ? " on service start" : "");
            healthIndicatorService.changeOccurred();
            processFileChanges(startup ? HIGHER_OR_SAME_VERSION : HIGHER_VERSION_ONLY);
        }
    }

    protected XContentParser createParser(InputStream stream) throws IOException {
        return JSON.xContent().createParser(XContentParserConfiguration.EMPTY, stream);
    }

    private void processFileChanges(ReservedStateVersionCheck versionCheck) throws IOException, InterruptedException, ExecutionException {
        PlainActionFuture<Void> completion = new PlainActionFuture<>();
        try (var bis = new BufferedInputStream(Files.newInputStream(watchedFile)); var parser = createParser(bis)) {
            stateService.process(NAMESPACE, parser, versionCheck, (e) -> completeProcessing(e, completion));
        }
        completion.get();
    }

    protected void completeProcessing(Exception e, PlainActionFuture<Void> completion) {
        if (e != null) {
            healthIndicatorService.failureOccurred(e.toString());
            completion.onFailure(e);
        } else {
            completion.onResponse(null);
            healthIndicatorService.successOccurred();
        }
    }

    @Override
    protected void onProcessFileChangesException(Path file, Exception e) {
        if (e instanceof ExecutionException) {
            var cause = e.getCause();
            if (cause instanceof FailedToCommitClusterStateException) {
                logger().error(Strings.format("Unable to commit cluster state while processing file [%s]", file), e);
                return;
            } else if (cause instanceof XContentParseException) {
                logger().error(Strings.format("Unable to parse settings from file [%s]", file), e);
                return;
            } else if (cause instanceof NotMasterException) {
                logger().error(Strings.format("Node is no longer master while processing file [%s]", file), e);
                return;
            }
        }

        super.onProcessFileChangesException(file, e);
    }

    @Override
    protected void processInitialFilesMissing() throws ExecutionException, InterruptedException {
        PlainActionFuture<ActionResponse.Empty> completion = new PlainActionFuture<>();
        logger().info("setting file [{}] not found, initializing [{}] as empty", watchedFile, NAMESPACE);
        stateService.initEmpty(NAMESPACE, completion);
        completion.get();
    }

    public static class FileSettingsHealthIndicatorService implements HealthIndicatorService {
        static final String NAME = "file_settings";
        static final String INACTIVE_SYMPTOM = "File-based settings are inactive";
        static final String NO_CHANGES_SYMPTOM = "No file-based setting changes have occurred";
        static final String SUCCESS_SYMPTOM = "The most recent file-based settings were applied successfully";
        static final String FAILURE_SYMPTOM = "The most recent file-based settings encountered an error";

        static final List<HealthIndicatorImpact> STALE_SETTINGS_IMPACT = List.of(
            new HealthIndicatorImpact(
                NAME,
                "stale",
                3,
                "The most recent file-based settings changes have not been applied.",
                List.of(DEPLOYMENT_MANAGEMENT)
            )
        );

        /**
         * We want a length limit so we don't blow past the indexing limit in the case of a long description string.
         * This is an {@code OperatorDynamic} setting so that if the truncation hampers troubleshooting efforts,
         * the operator could override it and retry the operation without necessarily restarting the cluster.
         */
        public static final String DESCRIPTION_LENGTH_LIMIT_KEY = "fileSettings.descriptionLengthLimit";
        static final Setting<Integer> DESCRIPTION_LENGTH_LIMIT = Setting.intSetting(
            DESCRIPTION_LENGTH_LIMIT_KEY,
            100,
            1, // Need room for the ellipsis
            Setting.Property.OperatorDynamic
        );

        private final Settings settings;
        private boolean isActive = false;
        private long changeCount = 0;
        private long failureStreak = 0;
        private String mostRecentFailure = null;

        public FileSettingsHealthIndicatorService(Settings settings) {
            this.settings = settings;
        }

        public synchronized void startOccurred() {
            isActive = true;
            failureStreak = 0;
        }

        public synchronized void stopOccurred() {
            isActive = false;
            mostRecentFailure = null;
        }

        public synchronized void changeOccurred() {
            ++changeCount;
        }

        public synchronized void successOccurred() {
            failureStreak = 0;
            mostRecentFailure = null;
        }

        public synchronized void failureOccurred(String description) {
            ++failureStreak;
            mostRecentFailure = limitLength(description);
        }

        private String limitLength(String description) {
            int descriptionLengthLimit = DESCRIPTION_LENGTH_LIMIT.get(settings);
            if (description.length() > descriptionLengthLimit) {
                return description.substring(0, descriptionLengthLimit - 1) + "…";
            } else {
                return description;
            }
        }

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public synchronized HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
            if (isActive == false) {
                return createIndicator(GREEN, INACTIVE_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
            }
            if (0 == changeCount) {
                return createIndicator(GREEN, NO_CHANGES_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
            }
            if (0 == failureStreak) {
                return createIndicator(GREEN, SUCCESS_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
            } else {
                return createIndicator(
                    YELLOW,
                    FAILURE_SYMPTOM,
                    new SimpleHealthIndicatorDetails(Map.of("failure_streak", failureStreak, "most_recent_failure", mostRecentFailure)),
                    STALE_SETTINGS_IMPACT,
                    List.of()
                );
            }
        }
    }
}
