/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.license.internal.TrialLicenseVersion;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Service responsible for managing {@link LicensesMetadata}.
 * <p>
 * On the master node, the service handles updating the cluster state when a new license is registered.
 * It also listens on all nodes for cluster state updates, and updates {@link XPackLicenseState} when
 * the license changes are detected in the cluster state.
 */
public class ClusterStateLicenseService extends AbstractLifecycleComponent
    implements
        MutableLicenseService,
        ClusterStateListener,
        SchedulerEngine.Listener {
    private static final Logger logger = LogManager.getLogger(ClusterStateLicenseService.class);

    private final Settings settings;

    private final ClusterService clusterService;
    private final FeatureService featureService;

    /**
     * The xpack feature state to update when license changes are made.
     */
    private final XPackLicenseState xPacklicenseState;

    /**
     * Currently active license
     */
    private final AtomicReference<License> currentLicenseHolder = new AtomicReference<>();
    private final SchedulerEngine scheduler;
    private final Clock clock;

    /**
     * Callbacks to notify relative to license expiry
     */
    private final List<ExpirationCallback> expirationCallbacks = new ArrayList<>();

    /**
     * Which license types are permitted to be uploaded to the cluster
     * @see LicenseSettings#ALLOWED_LICENSE_TYPES_SETTING
     */
    private final List<License.LicenseType> allowedLicenseTypes;

    private final MasterServiceTaskQueue<StartTrialClusterTask> startTrialTaskQueue;
    private final MasterServiceTaskQueue<StartBasicClusterTask> startBasicTaskQueue;

    public static final String LICENSE_JOB = "licenseJob";

    private static final String ACKNOWLEDGEMENT_HEADER = "This license update requires acknowledgement. To acknowledge the license, "
        + "please read the following messages and update the license again, this time with the \"acknowledge=true\" parameter:";

    @SuppressWarnings("this-escape")
    public ClusterStateLicenseService(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        Clock clock,
        XPackLicenseState xPacklicenseState,
        FeatureService featureService
    ) {
        this.settings = settings;
        this.clusterService = clusterService;
        this.featureService = featureService;
        this.startTrialTaskQueue = clusterService.createTaskQueue(
            "license-service-start-trial",
            Priority.NORMAL,
            new StartTrialClusterTask.Executor()
        );
        this.startBasicTaskQueue = clusterService.createTaskQueue(
            "license-service-start-basic",
            Priority.NORMAL,
            new StartBasicClusterTask.Executor()
        );
        this.clock = clock;
        this.scheduler = new SchedulerEngine(settings, clock);
        this.xPacklicenseState = xPacklicenseState;
        this.allowedLicenseTypes = LicenseSettings.ALLOWED_LICENSE_TYPES_SETTING.get(settings);
        this.scheduler.register(this);
        populateExpirationCallbacks();

        threadPool.scheduleWithFixedDelay(xPacklicenseState::cleanupUsageTracking, TimeValue.timeValueHours(1), threadPool.generic());
    }

    private void logExpirationWarning(long expirationMillis, boolean expired) {
        logger.warn("{}", buildExpirationMessage(expirationMillis, expired));
    }

    CharSequence buildExpirationMessage(long expirationMillis, boolean expired) {
        String expiredMsg = expired ? "expired" : "will expire";
        String general = LoggerMessageFormat.format(null, """
            License [{}] on [{}].
            # If you have a new license, please update it. Otherwise, please reach out to
            # your support contact.
            #\s""", expiredMsg, LicenseUtils.DATE_FORMATTER.formatMillis(expirationMillis));
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
        return builder;
    }

    private void populateExpirationCallbacks() {
        expirationCallbacks.add(new ExpirationCallback.Pre(days(0), days(25), days(1)) {
            @Override
            public void on(License license) {
                logExpirationWarning(LicenseUtils.getExpiryDate(license), false);
            }
        });
        expirationCallbacks.add(new ExpirationCallback.Post(days(0), null, TimeValue.timeValueMinutes(10)) {
            @Override
            public void on(License license) {
                logExpirationWarning(LicenseUtils.getExpiryDate(license), true);
            }
        });
    }

    /**
     * Registers new license in the cluster
     * Master only operation. Installs a new license on the master provided it is VALID
     */
    @Override
    public void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener) {
        final License newLicense = request.license();
        final long now = clock.millis();
        if (LicenseVerifier.verifyLicense(newLicense) == false || newLicense.issueDate() > now || newLicense.startDate() > now) {
            listener.onResponse(new PutLicenseResponse(true, LicensesStatus.INVALID));
            return;
        }
        final License.LicenseType licenseType;
        try {
            licenseType = License.LicenseType.resolve(newLicense);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (licenseType == License.LicenseType.BASIC) {
            listener.onFailure(new IllegalArgumentException("Registering basic licenses is not allowed."));
        } else if (isAllowedLicenseType(licenseType) == false) {
            listener.onFailure(
                new IllegalArgumentException("Registering [" + licenseType.getTypeName() + "] licenses is not allowed on this cluster")
            );
        } else if (LicenseUtils.getExpiryDate(newLicense) < now) {
            listener.onResponse(new PutLicenseResponse(true, LicensesStatus.EXPIRED));
        } else {
            if (request.acknowledged() == false) {
                // TODO: ack messages should be generated on the master, since another node's cluster state may be behind...
                final License currentLicense = getLicense();
                if (currentLicense != null) {
                    Map<String, String[]> acknowledgeMessages = LicenseUtils.getAckMessages(newLicense, currentLicense);
                    if (acknowledgeMessages.isEmpty() == false) {
                        // needs acknowledgement
                        listener.onResponse(
                            new PutLicenseResponse(false, LicensesStatus.VALID, ACKNOWLEDGEMENT_HEADER, acknowledgeMessages)
                        );
                        return;
                    }
                }
            }

            // This check would be incorrect if "basic" licenses were allowed here
            // because the defaults there mean that security can be "off", even if the setting is "on"
            // BUT basic licenses are explicitly excluded earlier in this method, so we don't need to worry
            if (XPackSettings.SECURITY_ENABLED.get(settings)) {
                if (XPackSettings.FIPS_MODE_ENABLED.get(settings)
                    && false == XPackLicenseState.isFipsAllowedForOperationMode(newLicense.operationMode())) {
                    throw new IllegalStateException(
                        "Cannot install a [" + newLicense.operationMode() + "] license unless FIPS mode is disabled"
                    );
                }
            }

            submitUnbatchedTask("register license [" + newLicense.uid() + "]", new AckedClusterStateUpdateTask(request, listener) {
                @Override
                protected PutLicenseResponse newResponse(boolean acknowledged) {
                    return new PutLicenseResponse(acknowledged, LicensesStatus.VALID);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
                    int maxCompatibleLicenseVersion = LicenseUtils.getMaxCompatibleLicenseVersion();
                    if (maxCompatibleLicenseVersion < newLicense.version()) {
                        throw new IllegalStateException(
                            LoggerMessageFormat.format(
                                "The provided license is of version [{}] but this node is only compatible with version [{}] "
                                    + "licences or older",
                                newLicense.version(),
                                maxCompatibleLicenseVersion
                            )
                        );
                    }
                    Metadata currentMetadata = currentState.metadata();
                    LicensesMetadata licensesMetadata = currentMetadata.custom(LicensesMetadata.TYPE);
                    TrialLicenseVersion trialVersion = null;
                    if (licensesMetadata != null) {
                        trialVersion = licensesMetadata.getMostRecentTrialVersion();
                    }
                    Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                    mdBuilder.putCustom(LicensesMetadata.TYPE, new LicensesMetadata(newLicense, trialVersion));
                    return ClusterState.builder(currentState).metadata(mdBuilder).build();
                }
            });
        }
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    private boolean isAllowedLicenseType(License.LicenseType type) {
        logger.debug("Checking license [{}] against allowed license types: {}", type, allowedLicenseTypes);
        return allowedLicenseTypes.contains(type);
    }

    private static TimeValue days(int days) {
        return TimeValue.timeValueHours(days * 24);
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        final LicensesMetadata licensesMetadata = getLicensesMetadata();
        if (licensesMetadata != null) {
            final License license = licensesMetadata.getLicense();
            if (event.jobName().equals(LICENSE_JOB)) {
                updateXPackLicenseState(license);
            } else if (event.jobName().startsWith(ExpirationCallback.EXPIRATION_JOB_PREFIX)) {
                expirationCallbacks.stream()
                    .filter(expirationCallback -> expirationCallback.getId().equals(event.jobName()))
                    .forEach(expirationCallback -> expirationCallback.on(license));
            }
        }
    }

    /**
     * Remove license from the cluster state metadata
     */
    @Override
    public void removeLicense(TimeValue masterNodeTimeout, TimeValue ackTimeout, ActionListener<? extends AcknowledgedResponse> listener) {
        final PostStartBasicRequest startBasicRequest = new PostStartBasicRequest(masterNodeTimeout, ackTimeout).acknowledge(true);
        @SuppressWarnings("unchecked")
        final StartBasicClusterTask task = new StartBasicClusterTask(
            logger,
            clusterService.getClusterName().value(),
            clock,
            startBasicRequest,
            "delete license",
            (ActionListener<PostStartBasicResponse>) listener
        );
        startBasicTaskQueue.submitTask(task.getDescription(), task, null); // TODO should pass in request.masterNodeTimeout() here
    }

    @Override
    public License getLicense() {
        final License license = getLicense(clusterService.state().metadata());
        return license == LicensesMetadata.LICENSE_TOMBSTONE ? null : license;
    }

    private LicensesMetadata getLicensesMetadata() {
        return this.clusterService.state().metadata().custom(LicensesMetadata.TYPE);
    }

    @Override
    public void startTrialLicense(PostStartTrialRequest request, final ActionListener<PostStartTrialResponse> listener) {
        License.LicenseType requestedType = License.LicenseType.parse(request.getType());
        if (LicenseSettings.VALID_TRIAL_TYPES.contains(requestedType) == false) {
            throw new IllegalArgumentException(
                "Cannot start trial of type ["
                    + requestedType.getTypeName()
                    + "]. Valid trial types are ["
                    + LicenseSettings.VALID_TRIAL_TYPES.stream()
                        .map(License.LicenseType::getTypeName)
                        .sorted()
                        .collect(Collectors.joining(","))
                    + "]"
            );
        }
        startTrialTaskQueue.submitTask(
            StartTrialClusterTask.TASK_SOURCE,
            new StartTrialClusterTask(logger, clusterService.getClusterName().value(), clock, featureService, request, listener),
            null             // TODO should pass in request.masterNodeTimeout() here
        );
    }

    @Override
    public void startBasicLicense(PostStartBasicRequest request, final ActionListener<PostStartBasicResponse> listener) {
        StartBasicClusterTask task = new StartBasicClusterTask(
            logger,
            clusterService.getClusterName().value(),
            clock,
            request,
            "start basic license",
            listener
        );
        startBasicTaskQueue.submitTask(task.getDescription(), task, null); // TODO should pass in request.masterNodeTimeout() here
    }

    /**
     * Master-only operation to generate a one-time global self generated license.
     * The self generated license is only generated and stored if the current cluster state metadata
     * has no existing license. If the cluster currently has a basic license that has an expiration date,
     * a new basic license with no expiration date is generated.
     */
    private void registerOrUpdateSelfGeneratedLicense() {
        submitUnbatchedTask(
            StartupSelfGeneratedLicenseTask.TASK_SOURCE,
            new StartupSelfGeneratedLicenseTask(settings, clock, clusterService)
        );
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clusterService.addListener(this);
        scheduler.start(Collections.emptyList());
        logger.debug("initializing license state");
        if (clusterService.lifecycleState() == Lifecycle.State.STARTED) {
            final ClusterState clusterState = clusterService.state();
            if (clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false
                && clusterState.nodes().getMasterNode() != null
                && XPackPlugin.isReadyForXPackCustomMetadata(clusterState)) {
                final LicensesMetadata currentMetadata = clusterState.metadata().custom(LicensesMetadata.TYPE);
                boolean noLicense = currentMetadata == null || currentMetadata.getLicense() == null;
                if (clusterState.getNodes().isLocalNodeElectedMaster()
                    && (noLicense || LicenseUtils.licenseNeedsExtended(currentMetadata.getLicense()))) {
                    // triggers a cluster changed event eventually notifying the current licensee
                    registerOrUpdateSelfGeneratedLicense();
                }
            }
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        clusterService.removeListener(this);
        scheduler.stop();
        // clear current license
        currentLicenseHolder.set(null);
    }

    @Override
    protected void doClose() throws ElasticsearchException {}

    /**
     * When there is no global block on {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK}
     * notify licensees and issue auto-generated license if no license has been installed/issued yet.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState previousClusterState = event.previousState();
        final ClusterState currentClusterState = event.state();
        if (currentClusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
            final LicensesMetadata prevLicensesMetadata = previousClusterState.getMetadata().custom(LicensesMetadata.TYPE);
            final LicensesMetadata currentLicensesMetadata = currentClusterState.getMetadata().custom(LicensesMetadata.TYPE);
            // notify all interested plugins
            if (previousClusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || prevLicensesMetadata == null) {
                if (currentLicensesMetadata != null) {
                    logger.debug("state recovered: previous license [{}]", prevLicensesMetadata);
                    logger.debug("state recovered: current license [{}]", currentLicensesMetadata);
                    onUpdate(currentLicensesMetadata);
                } else {
                    logger.trace("state recovered: no current license");
                }
            } else if (prevLicensesMetadata.equals(currentLicensesMetadata) == false) {
                logger.debug("previous [{}]", prevLicensesMetadata);
                logger.debug("current [{}]", currentLicensesMetadata);
                onUpdate(currentLicensesMetadata);
            } else {
                logger.trace("license unchanged [{}]", currentLicensesMetadata);
            }
            maybeRegisterOrUpdateLicense(previousClusterState, currentClusterState);
        } else if (logger.isDebugEnabled()) {
            logger.debug("skipped license notifications reason: [{}]", GatewayService.STATE_NOT_RECOVERED_BLOCK);
        }
    }

    private void updateXPackLicenseState(License license) {
        if (license == LicensesMetadata.LICENSE_TOMBSTONE) {
            // implies license has been explicitly deleted
            xPacklicenseState.update(LicenseUtils.getXPackLicenseStatus(license, clock));
        } else if (license != null) {
            XPackLicenseStatus xPackLicenseStatus = LicenseUtils.getXPackLicenseStatus(license, clock);
            xPacklicenseState.update(xPackLicenseStatus);
            if (xPackLicenseStatus.active()) {
                logger.debug("license [{}] - valid", license.uid());
            } else {
                logger.warn("license [{}] - expired", license.uid());
            }
        }
    }

    private void maybeRegisterOrUpdateLicense(ClusterState previousClusterState, ClusterState currentClusterState) {
        final LicensesMetadata prevLicensesMetadata = previousClusterState.getMetadata().custom(LicensesMetadata.TYPE);
        final LicensesMetadata currentLicensesMetadata = currentClusterState.getMetadata().custom(LicensesMetadata.TYPE);
        License currentLicense = null;
        boolean noLicenseInPrevMetadata = prevLicensesMetadata == null || prevLicensesMetadata.getLicense() == null;
        if (noLicenseInPrevMetadata == false) {
            currentLicense = prevLicensesMetadata.getLicense();
        }
        boolean noLicenseInCurrentMetadata = (currentLicensesMetadata == null || currentLicensesMetadata.getLicense() == null);
        if (noLicenseInCurrentMetadata == false) {
            currentLicense = currentLicensesMetadata.getLicense();
        }
        boolean noLicense = noLicenseInPrevMetadata && noLicenseInCurrentMetadata;
        // auto-generate license if no licenses ever existed or if the current license is basic and
        // needs extended or if the license signature needs to be updated. this will trigger a subsequent cluster changed event
        if (currentClusterState.getNodes().isLocalNodeElectedMaster()
            && (noLicense || LicenseUtils.licenseNeedsExtended(currentLicense) || LicenseUtils.signatureNeedsUpdate(currentLicense))) {
            registerOrUpdateSelfGeneratedLicense();
        }
    }

    /**
     * Notifies registered licensees of license state change and/or new active license
     * based on the license in <code>currentLicensesMetadata</code>.
     * Additionally schedules license expiry notifications and event callbacks
     * relative to the current license's expiry
     */
    private void onUpdate(final LicensesMetadata currentLicensesMetadata) {
        final License license = getLicenseFromLicensesMetadata(currentLicensesMetadata);
        // first update the XPackLicenseState
        updateXPackLicenseState(license);
        // license can be null if the trial license is yet to be auto-generated
        // in this case, it is a no-op
        if (license != null) {
            final License previousLicense = currentLicenseHolder.getAndSet(license);
            if (license.equals(previousLicense) == false) {
                // then register periodic job to update the XPackLicenseState with the latest expiration message
                scheduler.add(new SchedulerEngine.Job(LICENSE_JOB, nextLicenseCheck(license)));
                for (ExpirationCallback expirationCallback : expirationCallbacks) {
                    scheduler.add(
                        new SchedulerEngine.Job(
                            expirationCallback.getId(),
                            (startTime, now) -> expirationCallback.nextScheduledTimeForExpiry(
                                LicenseUtils.getExpiryDate(license),
                                startTime,
                                now
                            )
                        )
                    );
                }
                logger.info("license [{}] mode [{}] - valid", license.uid(), license.operationMode().name().toLowerCase(Locale.ROOT));
            }
        }
    }

    // pkg private for tests
    SchedulerEngine.Schedule nextLicenseCheck(License license) {
        final long licenseIssueDate = license.issueDate();
        final long licenseExpiryDate = LicenseUtils.getExpiryDate(license);
        return (startTime, time) -> {
            if (time < licenseIssueDate) {
                // when we encounter a license with a future issue date
                // which can happen with autogenerated license,
                // we want to schedule a notification on the license issue date
                // so the license is notified once it is valid
                // see https://github.com/elastic/x-plugins/issues/983
                return licenseIssueDate;
            } else if (time < licenseExpiryDate) {
                // Re-check the license every day during the warning period up to the license expiration.
                // This will cause the warning message to be updated that is emitted on soon-expiring license use.
                long nextTime = licenseExpiryDate - LicenseSettings.LICENSE_EXPIRATION_WARNING_PERIOD.getMillis();
                while (nextTime <= time) {
                    nextTime += TimeValue.timeValueDays(1).getMillis();
                }
                return nextTime;
            }
            return -1; // license is expired, no need to check again
        };
    }

    public License getLicense(final Metadata metadata) {
        final LicensesMetadata licensesMetadata = metadata.custom(LicensesMetadata.TYPE);
        return getLicenseFromLicensesMetadata(licensesMetadata);
    }

    // visible for tests
    @Nullable
    License getLicenseFromLicensesMetadata(@Nullable final LicensesMetadata metadata) {
        if (metadata != null) {
            License license = metadata.getLicense();
            if (license == LicensesMetadata.LICENSE_TOMBSTONE) {
                return license;
            } else if (license != null) {
                if (license.verified()) {
                    return license;
                } else {
                    // this is an "error" level because an unverified license should not be present in the cluster state in the first place
                    logger.error(
                        "{} with uid [{}] failed verification on the local node.",
                        License.isAutoGeneratedLicense(license.signature()) ? "Autogenerated license" : "License",
                        license.uid()
                    );
                }
            }
        }
        return null;
    }

}
