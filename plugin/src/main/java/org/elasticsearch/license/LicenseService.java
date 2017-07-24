/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service responsible for managing {@link LicensesMetaData}.
 * <p>
 * On the master node, the service handles updating the cluster state when a new license is registered.
 * It also listens on all nodes for cluster state updates, and updates {@link XPackLicenseState} when
 * the license changes are detected in the cluster state.
 */
public class LicenseService extends AbstractLifecycleComponent implements ClusterStateListener, SchedulerEngine.Listener {

    public static final Setting<String> SELF_GENERATED_LICENSE_TYPE = new Setting<>("xpack.license.self_generated.type",
            (s) -> "trial", (s) -> {
        if (validSelfGeneratedType(s)) {
            return s;
        } else {
            throw new IllegalArgumentException("Illegal self generated license type [" + s + "]. Must be trial or basic.");
        }
    }, Setting.Property.NodeScope);

    // pkg private for tests
    static final TimeValue SELF_GENERATED_LICENSE_DURATION = TimeValue.timeValueHours(30 * 24);
    /**
     * Duration of grace period after a license has expired
     */
    static final TimeValue GRACE_PERIOD_DURATION = days(7);

    private final ClusterService clusterService;

    /**
     * The xpack feature state to update when license changes are made.
     */
    private final XPackLicenseState licenseState;

    /**
     * Currently active license
     */
    private final AtomicReference<License> currentLicense = new AtomicReference<>();
    private SchedulerEngine scheduler;
    private final Clock clock;

    /**
     * File watcher for operation mode changes
     */
    private final OperationModeFileWatcher operationModeFileWatcher;

    /**
     * Callbacks to notify relative to license expiry
     */
    private List<ExpirationCallback> expirationCallbacks = new ArrayList<>();

    /**
     * Max number of nodes licensed by generated trial license
     */
    private int selfGeneratedLicenseMaxNodes = 1000;

    public static final String LICENSE_JOB = "licenseJob";

    private static final FormatDateTimeFormatter DATE_FORMATTER = Joda.forPattern("EEEE, MMMMM dd, yyyy", Locale.ROOT);

    private static final String ACKNOWLEDGEMENT_HEADER = "This license update requires acknowledgement. To acknowledge the license, " +
            "please read the following messages and update the license again, this time with the \"acknowledge=true\" parameter:";

    public LicenseService(Settings settings, ClusterService clusterService, Clock clock, Environment env,
                          ResourceWatcherService resourceWatcherService, XPackLicenseState licenseState) {
        super(settings);
        this.clusterService = clusterService;
        this.clock = clock;
        this.scheduler = new SchedulerEngine(clock);
        this.licenseState = licenseState;
        this.operationModeFileWatcher = new OperationModeFileWatcher(resourceWatcherService,
                XPackPlugin.resolveConfigFile(env, "license_mode"), logger, () -> updateLicenseState(getLicense()));
        this.scheduler.register(this);
        populateExpirationCallbacks();
    }

    private void logExpirationWarning(long expirationMillis, boolean expired) {
        String expiredMsg = expired ? "expired" : "will expire";
        String general = LoggerMessageFormat.format(null, "\n" +
                "#\n" +
                "# License [{}] on [{}]. If you have a new license, please update it.\n" +
                "# Otherwise, please reach out to your support contact.\n" +
                "# ", expiredMsg, DATE_FORMATTER.printer().print(expirationMillis));
        if (expired) {
            general = general.toUpperCase(Locale.ROOT);
        }
        StringBuilder builder = new StringBuilder(general);
        builder.append(System.lineSeparator());
        if (expired) {
            builder.append("# COMMERCIAL PLUGINS OPERATING WITH REDUCED FUNCTIONALITY");
        } else {
            builder.append("# Commercial plugins operate with reduced functionality on license expiration:");
        }
        XPackLicenseState.EXPIRATION_MESSAGES.forEach((feature, messages) -> {
            if (messages.length > 0) {
                builder.append(System.lineSeparator());
                builder.append("# - ");
                builder.append(feature);
                for (String message : messages) {
                    builder.append(System.lineSeparator());
                    builder.append("#  - ");
                    builder.append(message);
                }
            }
        });
        logger.warn("{}", builder);
    }

    private void populateExpirationCallbacks() {
        expirationCallbacks.add(new ExpirationCallback.Pre(days(7), days(25), days(1)) {
            @Override
            public void on(License license) {
                logExpirationWarning(license.expiryDate(), false);
            }
        });
        expirationCallbacks.add(new ExpirationCallback.Pre(days(0), days(7), TimeValue.timeValueMinutes(10)) {
            @Override
            public void on(License license) {
                logExpirationWarning(license.expiryDate(), false);
            }
        });
        expirationCallbacks.add(new ExpirationCallback.Post(days(0), null, TimeValue.timeValueMinutes(10)) {
            @Override
            public void on(License license) {
                // logged when grace period begins
                logExpirationWarning(license.expiryDate(), true);
            }
        });
    }

    /**
     * Registers new license in the cluster
     * Master only operation. Installs a new license on the master provided it is VALID
     */
    public void registerLicense(final PutLicenseRequest request, final ActionListener<PutLicenseResponse> listener) {
        final License newLicense = request.license();
        final long now = clock.millis();
        if (!LicenseVerifier.verifyLicense(newLicense) || newLicense.issueDate() > now || newLicense.startDate() > now) {
            listener.onResponse(new PutLicenseResponse(true, LicensesStatus.INVALID));
        } else if (newLicense.expiryDate() < now) {
            listener.onResponse(new PutLicenseResponse(true, LicensesStatus.EXPIRED));
        } else {
            if (!request.acknowledged()) {
                // TODO: ack messages should be generated on the master, since another node's cluster state may be behind...
                final License currentLicense = getLicense();
                if (currentLicense != null) {
                    Map<String, String[]> acknowledgeMessages = new HashMap<>();
                    if (!License.isAutoGeneratedLicense(currentLicense.signature()) // current license is not auto-generated
                            && currentLicense.issueDate() > newLicense.issueDate()) { // and has a later issue date
                        acknowledgeMessages.put("license", new String[]{
                                "The new license is older than the currently installed license. " +
                                        "Are you sure you want to override the current license?"});
                    }
                    XPackLicenseState.ACKNOWLEDGMENT_MESSAGES.forEach((feature, ackMessages) -> {
                        String[] messages = ackMessages.apply(currentLicense.operationMode(), newLicense.operationMode());
                        if (messages.length > 0) {
                            acknowledgeMessages.put(feature, messages);
                        }
                    });
                    if (acknowledgeMessages.isEmpty() == false) {
                        // needs acknowledgement
                        listener.onResponse(new PutLicenseResponse(false, LicensesStatus.VALID, ACKNOWLEDGEMENT_HEADER,
                                acknowledgeMessages));
                        return;
                    }
                }
            }
            clusterService.submitStateUpdateTask("register license [" + newLicense.uid() + "]", new
                    AckedClusterStateUpdateTask<PutLicenseResponse>(request, listener) {
                        @Override
                        protected PutLicenseResponse newResponse(boolean acknowledged) {
                            return new PutLicenseResponse(acknowledged, LicensesStatus.VALID);
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                            mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(newLicense));
                            return ClusterState.builder(currentState).metaData(mdBuilder).build();
                        }
                    });
        }
    }


    static TimeValue days(int days) {
        return TimeValue.timeValueHours(days * 24);
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        final LicensesMetaData licensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        if (licensesMetaData != null) {
            final License license = licensesMetaData.getLicense();
            if (event.getJobName().equals(LICENSE_JOB)) {
                updateLicenseState(license);
            } else if (event.getJobName().startsWith(ExpirationCallback.EXPIRATION_JOB_PREFIX)) {
                expirationCallbacks.stream()
                        .filter(expirationCallback -> expirationCallback.getId().equals(event.getJobName()))
                        .forEach(expirationCallback -> expirationCallback.on(license));
            }
        }
    }

    /**
     * Remove license from the cluster state metadata
     */
    public void removeLicense(final DeleteLicenseRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask("delete license",
                new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
                    @Override
                    protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                        return new ClusterStateUpdateResponse(acknowledged);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        MetaData metaData = currentState.metaData();
                        final LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);
                        if (currentLicenses.getLicense() != LicensesMetaData.LICENSE_TOMBSTONE) {
                            MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                            mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(LicensesMetaData.LICENSE_TOMBSTONE));
                            return ClusterState.builder(currentState).metaData(mdBuilder).build();
                        } else {
                            return currentState;
                        }
                    }
                });
    }

    public License getLicense() {
        final License license = getLicense(clusterService.state().metaData().custom(LicensesMetaData.TYPE));
        return license == LicensesMetaData.LICENSE_TOMBSTONE ? null : license;
    }

    /**
     * Master-only operation to generate a one-time global trial license.
     * The trial license is only generated and stored if the current cluster state metaData
     * has no signed/trial license
     */
    private void registerTrialLicense() {
        clusterService.submitStateUpdateTask("generate trial license for [" + SELF_GENERATED_LICENSE_DURATION + "]",
                new ClusterStateUpdateTask() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        LicensesMetaData licensesMetaData = newState.metaData().custom(LicensesMetaData.TYPE);
                        if (logger.isDebugEnabled()) {
                            logger.debug("registered trial license: {}", licensesMetaData);
                        }
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        final MetaData metaData = currentState.metaData();
                        final LicensesMetaData currentLicensesMetaData = metaData.custom(LicensesMetaData.TYPE);
                        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                        // do not generate a trial license if any license is present
                        if (currentLicensesMetaData == null) {
                            long issueDate = clock.millis();
                            String type = SELF_GENERATED_LICENSE_TYPE.get(settings);
                            if (validSelfGeneratedType(type) == false) {
                                throw new IllegalArgumentException("Illegal self generated license type [" + type +
                                        "]. Must be trial or basic.");
                            }
                            License.Builder specBuilder = License.builder()
                                    .uid(UUID.randomUUID().toString())
                                    .issuedTo(clusterService.getClusterName().value())
                                    .maxNodes(selfGeneratedLicenseMaxNodes)
                                    .issueDate(issueDate)
                                    .type(type)
                                    .expiryDate(issueDate + SELF_GENERATED_LICENSE_DURATION.getMillis());
                            License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder);
                            mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(selfGeneratedLicense));
                            return ClusterState.builder(currentState).metaData(mdBuilder).build();
                        }
                        return currentState;
                    }

                    @Override
                    public void onFailure(String source, @Nullable Exception e) {
                        logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
                    }

                });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clusterService.addListener(this);
        scheduler.start(Collections.emptyList());
        logger.debug("initializing license state");
        if (clusterService.lifecycleState() == Lifecycle.State.STARTED) {
            final ClusterState clusterState = clusterService.state();
            if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false &&
                    clusterState.nodes().getMasterNode() != null) {
                final LicensesMetaData currentMetaData = clusterState.metaData().custom(LicensesMetaData.TYPE);
                if (clusterState.getNodes().isLocalNodeElectedMaster() &&
                        (currentMetaData == null || currentMetaData.getLicense() == null)) {
                    // triggers a cluster changed event
                    // eventually notifying the current licensee
                    registerTrialLicense();
                }
            }
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        clusterService.removeListener(this);
        scheduler.stop();
        // clear current license
        currentLicense.set(null);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    /**
     * When there is no global block on {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK}
     * notify licensees and issue auto-generated license if no license has been installed/issued yet.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState previousClusterState = event.previousState();
        final ClusterState currentClusterState = event.state();
        if (!currentClusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            final LicensesMetaData prevLicensesMetaData = previousClusterState.getMetaData().custom(LicensesMetaData.TYPE);
            final LicensesMetaData currentLicensesMetaData = currentClusterState.getMetaData().custom(LicensesMetaData.TYPE);
            if (logger.isDebugEnabled()) {
                logger.debug("previous [{}]", prevLicensesMetaData);
                logger.debug("current [{}]", currentLicensesMetaData);
            }
            // notify all interested plugins
            if (previousClusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                    || prevLicensesMetaData == null) {
                if (currentLicensesMetaData != null) {
                    onUpdate(currentLicensesMetaData);
                }
            } else if (!prevLicensesMetaData.equals(currentLicensesMetaData)) {
                onUpdate(currentLicensesMetaData);
            }
            // auto-generate license if no licenses ever existed
            // this will trigger a subsequent cluster changed event
            if (currentClusterState.getNodes().isLocalNodeElectedMaster() &&
                    prevLicensesMetaData == null &&
                    (currentLicensesMetaData == null || currentLicensesMetaData.getLicense() == null)) {
                registerTrialLicense();
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("skipped license notifications reason: [{}]", GatewayService.STATE_NOT_RECOVERED_BLOCK);
        }
    }

    protected void updateLicenseState(final License license) {
        if (license == LicensesMetaData.LICENSE_TOMBSTONE) {
            // implies license has been explicitly deleted
            licenseState.update(License.OperationMode.MISSING, false);
            return;
        }
        if (license != null) {
            long time = clock.millis();
            boolean active = time >= license.issueDate() &&
                    time < license.expiryDate() + GRACE_PERIOD_DURATION.getMillis();
            licenseState.update(license.operationMode(), active);

            if (active) {
                if (time < license.expiryDate()) {
                    logger.debug("license [{}] - valid", license.uid());
                } else {
                    logger.warn("license [{}] - grace", license.uid());
                }
            } else {
                logger.warn("license [{}] - expired", license.uid());
            }
        }
    }

    /**
     * Notifies registered licensees of license state change and/or new active license
     * based on the license in <code>currentLicensesMetaData</code>.
     * Additionally schedules license expiry notifications and event callbacks
     * relative to the current license's expiry
     */
    void onUpdate(final LicensesMetaData currentLicensesMetaData) {
        final License license = getLicense(currentLicensesMetaData);
        // license can be null if the trial license is yet to be auto-generated
        // in this case, it is a no-op
        if (license != null) {
            final License previousLicense = currentLicense.get();
            if (license.equals(previousLicense) == false) {
                currentLicense.set(license);
                license.setOperationModeFileWatcher(operationModeFileWatcher);
                scheduler.add(new SchedulerEngine.Job(LICENSE_JOB, nextLicenseCheck(license)));
                for (ExpirationCallback expirationCallback : expirationCallbacks) {
                    scheduler.add(new SchedulerEngine.Job(expirationCallback.getId(),
                            (startTime, now) ->
                                    expirationCallback.nextScheduledTimeForExpiry(license.expiryDate(), startTime, now)));
                }
                if (previousLicense != null) {
                    // remove operationModeFileWatcher to gc the old license object
                    previousLicense.removeOperationModeFileWatcher();
                }
                logger.info("license [{}] mode [{}] - valid", license.uid(),
                        license.operationMode().name().toLowerCase(Locale.ROOT));
            }
            updateLicenseState(license);
        }
    }

    // pkg private for tests
    static SchedulerEngine.Schedule nextLicenseCheck(License license) {
        return (startTime, time) -> {
            if (time < license.issueDate()) {
                // when we encounter a license with a future issue date
                // which can happen with autogenerated license,
                // we want to schedule a notification on the license issue date
                // so the license is notificed once it is valid
                // see https://github.com/elastic/x-plugins/issues/983
                return license.issueDate();
            } else if (time < license.expiryDate()) {
                return license.expiryDate();
            } else if (time < license.expiryDate() + GRACE_PERIOD_DURATION.getMillis()) {
                return license.expiryDate() + GRACE_PERIOD_DURATION.getMillis();
            }
            return -1; // license is expired, no need to check again
        };
    }

    License getLicense(final LicensesMetaData metaData) {
        if (metaData != null) {
            License license = metaData.getLicense();
            if (license == LicensesMetaData.LICENSE_TOMBSTONE) {
                return license;
            } else if (license != null) {
                boolean autoGeneratedLicense = License.isAutoGeneratedLicense(license.signature());
                if ((autoGeneratedLicense && SelfGeneratedLicense.verify(license))
                        || (!autoGeneratedLicense && LicenseVerifier.verifyLicense(license))) {
                    return license;
                }
            }
        }
        return null;
    }

    private static boolean validSelfGeneratedType(String type) {
        return "basic".equals(type) || "trial".equals(type);
    }
}