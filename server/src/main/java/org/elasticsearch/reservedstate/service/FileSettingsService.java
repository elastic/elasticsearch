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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ReservedStateMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.file.MasterNodeFileWatchingService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

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
    private final FileSettingsHealthTracker healthIndicatorTracker;

    /**
     * Constructs the {@link FileSettingsService}
     *
     * @param clusterService so we can register ourselves as a cluster state change listener
     * @param stateService an instance of the immutable cluster state controller, so we can perform the cluster state changes
     * @param environment we need the environment to pull the location of the config and operator directories
     * @param healthIndicatorTracker tracks the success or failure of file-based settings operations
     */
    @SuppressWarnings("this-escape")
    public FileSettingsService(
        ClusterService clusterService,
        ReservedClusterStateService stateService,
        Environment environment,
        FileSettingsHealthTracker healthIndicatorTracker
    ) {
        super(clusterService, environment.configDir().toAbsolutePath().resolve(OPERATOR_DIRECTORY));
        this.watchedFile = watchedFileDir().resolve(SETTINGS_FILE_NAME);
        this.stateService = stateService;
        this.healthIndicatorTracker = healthIndicatorTracker;
    }

    protected Logger logger() {
        return logger;
    }

    public Path watchedFile() {
        return watchedFile;
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
        if (watching() && filesExists(watchedFile)) {
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
        healthIndicatorTracker.startOccurred();
        super.doStart();
    }

    @Override
    protected void doStop() {
        super.doStop();
        healthIndicatorTracker.stopOccurred();
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
            healthIndicatorTracker.changeOccurred();
            processFileChanges(startup ? HIGHER_OR_SAME_VERSION : HIGHER_VERSION_ONLY);
        }
    }

    protected XContentParser createParser(InputStream stream) throws IOException {
        return JSON.xContent().createParser(XContentParserConfiguration.EMPTY, stream);
    }

    private void processFileChanges(ReservedStateVersionCheck versionCheck) throws IOException, InterruptedException, ExecutionException {
        PlainActionFuture<Void> completion = new PlainActionFuture<>();
        try (var bis = new BufferedInputStream(filesNewInputStream(watchedFile)); var parser = createParser(bis)) {
            stateService.process(NAMESPACE, parser, versionCheck, (e) -> completeProcessing(e, completion));
        }
        completion.get();
    }

    protected void completeProcessing(Exception e, PlainActionFuture<Void> completion) {
        try {
            if (e != null) {
                healthIndicatorTracker.failureOccurred(e.toString());
                completion.onFailure(e);
            } else {
                completion.onResponse(null);
                healthIndicatorTracker.successOccurred();
            }
        } finally {
            logger().debug("Publishing to health node");
            healthIndicatorTracker.publish();
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

    public record FileSettingsHealthInfo(boolean isActive, long changeCount, long failureStreak, String mostRecentFailure)
        implements
            Writeable {

        /**
         * Indicates that no conclusions can be drawn about the health status.
         */
        public static final FileSettingsHealthInfo INDETERMINATE = new FileSettingsHealthInfo(false, 0L, 0, null);

        /**
         * Indicates that the health info system is active and no changes have occurred yet, so all is well.
         */
        public static final FileSettingsHealthInfo INITIAL_ACTIVE = new FileSettingsHealthInfo(true, 0L, 0, null);

        public FileSettingsHealthInfo(StreamInput in) throws IOException {
            this(in.readBoolean(), in.readVLong(), in.readVLong(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(isActive);
            out.writeVLong(changeCount);
            out.writeVLong(failureStreak);
            out.writeOptionalString(mostRecentFailure);
        }

        public FileSettingsHealthInfo inactive() {
            return new FileSettingsHealthInfo(false, changeCount, failureStreak, mostRecentFailure);
        }

        public FileSettingsHealthInfo changed() {
            return new FileSettingsHealthInfo(isActive, changeCount + 1, failureStreak, mostRecentFailure);
        }

        public FileSettingsHealthInfo successful() {
            return new FileSettingsHealthInfo(isActive, changeCount, 0, null);
        }

        public FileSettingsHealthInfo failed(String failureDescription) {
            return new FileSettingsHealthInfo(isActive, changeCount, failureStreak + 1, failureDescription);
        }
    }

    /**
     * Stateless service that maps a {@link FileSettingsHealthInfo} to a {@link HealthIndicatorResult}.
     */
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

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public synchronized HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
            return calculate(healthInfo.fileSettingsHealthInfo());
        }

        public HealthIndicatorResult calculate(FileSettingsHealthInfo info) {
            if (info.isActive() == false) {
                return createIndicator(GREEN, INACTIVE_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
            }
            if (0 == info.changeCount()) {
                return createIndicator(GREEN, NO_CHANGES_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
            }
            if (0 == info.failureStreak()) {
                return createIndicator(GREEN, SUCCESS_SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
            } else {
                return createIndicator(
                    YELLOW,
                    FAILURE_SYMPTOM,
                    new SimpleHealthIndicatorDetails(
                        Map.of("failure_streak", info.failureStreak(), "most_recent_failure", info.mostRecentFailure())
                    ),
                    STALE_SETTINGS_IMPACT,
                    List.of()
                );
            }
        }
    }

    /**
     * Houses the current {@link FileSettingsHealthInfo} and provides a means to <i>publish</i> it to the health node.
     */
    public static class FileSettingsHealthTracker {
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
        private final FileSettingsHealthIndicatorPublisher publisher;
        private FileSettingsHealthInfo currentInfo = FileSettingsHealthInfo.INDETERMINATE;

        public FileSettingsHealthTracker(Settings settings, FileSettingsHealthIndicatorPublisher publisher) {
            this.settings = settings;
            this.publisher = publisher;
        }

        public FileSettingsHealthInfo getCurrentInfo() {
            return currentInfo;
        }

        public synchronized void startOccurred() {
            currentInfo = FileSettingsHealthInfo.INITIAL_ACTIVE;
        }

        public synchronized void stopOccurred() {
            currentInfo = currentInfo.inactive();
        }

        public synchronized void changeOccurred() {
            currentInfo = currentInfo.changed();
        }

        public synchronized void successOccurred() {
            currentInfo = currentInfo.successful();
        }

        public synchronized void failureOccurred(String description) {
            currentInfo = currentInfo.failed(limitLength(description));
        }

        private String limitLength(String description) {
            int descriptionLengthLimit = DESCRIPTION_LENGTH_LIMIT.get(settings);
            if (description.length() > descriptionLengthLimit) {
                return description.substring(0, descriptionLengthLimit - 1) + "…";
            } else {
                return description;
            }
        }

        /**
         * Sends the current health info to the health node.
         */
        public void publish() {
            publisher.publish(
                currentInfo,
                ActionListener.wrap(
                    r -> logger.debug("Successfully published health indicator"),
                    e -> logger.warn("Failed to publish health indicator", e)
                )
            );
        }
    }

    public static class FileSettingsHealthIndicatorPublisherImpl implements FileSettingsHealthIndicatorPublisher {
        private final ClusterService clusterService;
        private final Client client;

        public FileSettingsHealthIndicatorPublisherImpl(ClusterService clusterService, Client client) {
            this.clusterService = clusterService;
            this.client = client;
        }

        public void publish(FileSettingsHealthInfo info, ActionListener<AcknowledgedResponse> actionListener) {
            DiscoveryNode currentHealthNode = HealthNode.findHealthNode(clusterService.state());
            if (currentHealthNode == null) {
                logger.debug(
                    "Unable to report file settings health because there is no health node in the cluster;"
                        + " will retry next time file settings health changes."
                );
            } else {
                logger.debug("Publishing file settings health indicators: [{}]", info);
                String localNode = clusterService.localNode().getId();
                client.execute(
                    UpdateHealthInfoCacheAction.INSTANCE,
                    new UpdateHealthInfoCacheAction.Request(localNode, info),
                    actionListener
                );
            }
        }
    }

    // the following methods are a workaround to ensure exclusive access for files
    // required by child watchers; this is required because we only check the caller's module
    // not the entire stack
    @Override
    protected boolean filesExists(Path path) {
        return Files.exists(path);
    }

    @Override
    protected boolean filesIsDirectory(Path path) {
        return Files.isDirectory(path);
    }

    @Override
    protected boolean filesIsSymbolicLink(Path path) {
        return Files.isSymbolicLink(path);
    }

    @Override
    protected <A extends BasicFileAttributes> A filesReadAttributes(Path path, Class<A> clazz) throws IOException {
        return Files.readAttributes(path, clazz);
    }

    @Override
    protected Stream<Path> filesList(Path dir) throws IOException {
        return Files.list(dir);
    }

    @Override
    protected Path filesSetLastModifiedTime(Path path, FileTime time) throws IOException {
        return Files.setLastModifiedTime(path, time);
    }

    @Override
    protected InputStream filesNewInputStream(Path path) throws IOException {
        return Files.newInputStream(path);
    }
}
