/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.LicenseVerifier;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service responsible for managing {@link LicensesMetaData}
 * Interfaces through which this is exposed are:
 * - LicensesManagerService - responsible for managing signed and one-time-trial licenses
 * - LicensesClientService - responsible for listener registration of consumer plugin(s)
 * <p>
 * Registration Scheme:
 * <p>
 * A consumer plugin is registered with {@link LicenseeRegistry#register(Licensee)}
 * This method can be called at any time during the life-cycle of the consumer plugin.
 * If the listener can not be registered immediately, it is queued up and registered on the first clusterChanged event with
 * no {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK} block
 * Upon successful registration, the listeners are notified appropriately using the notification scheme
 * <p>
 * Notification Scheme:
 * <p>
 * All registered listeners are notified of the current license upon registration or when a new license is installed in the cluster state.
 * When a new license is notified as enabled to the registered listener, a notification is scheduled at the time of license expiry.
 * Registered listeners are notified using {@link #notifyAndSchedule(LicensesMetaData)}
 */
@Singleton
public class LicensesService extends AbstractLifecycleComponent<LicensesService> implements ClusterStateListener, LicensesManagerService,
        LicenseeRegistry {

    public static final String REGISTER_TRIAL_LICENSE_ACTION_NAME = "internal:plugin/license/cluster/register_trial_license";

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    /**
     * Currently active consumers to notify to
     */
    private final List<InternalLicensee> registeredLicensees = new CopyOnWriteArrayList<>();

    /**
     * Currently active expiry notifications
     */
    private final Queue<ScheduledFuture> expiryNotifications = new ConcurrentLinkedQueue<>();

    /**
     * Currently active event notifications for every registered listener
     */
    private final Queue<ScheduledFuture> eventNotifications = new ConcurrentLinkedQueue<>();

    /**
     * Currently active license
     */
    private final AtomicReference<License> currentLicense = new AtomicReference<>();

    /**
     * Callbacks to notify relative to license expiry
     */
    private List<ExpirationCallback> expirationCallbacks = new ArrayList<>();

    /**
     * Duration of generated trial license
     */
    private TimeValue trialLicenseDuration = TimeValue.timeValueHours(30 * 24);

    /**
     * Max number of nodes licensed by generated trial license
     */
    private int trialLicenseMaxNodes = 1000;

    /**
     * Duration of grace period after a license has expired
     */
    private TimeValue gracePeriodDuration = days(7);

    private static final FormatDateTimeFormatter DATE_FORMATTER = Joda.forPattern("EEEE, MMMMM dd, yyyy", Locale.ROOT);

    private static final String ACKNOWLEDGEMENT_HEADER = "This license update requires acknowledgement. To acknowledge the license, " +
            "please read the following messages and update the license again, this time with the \"acknowledge=true\" parameter:";

    @Inject
    public LicensesService(Settings settings, ClusterService clusterService, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        if (DiscoveryNode.isMasterNode(settings)) {
            transportService.registerRequestHandler(REGISTER_TRIAL_LICENSE_ACTION_NAME, TransportRequest.Empty::new,
                    ThreadPool.Names.SAME, new RegisterTrialLicenseRequestHandler());
        }
        populateExpirationCallbacks();
    }

    private void populateExpirationCallbacks() {
        expirationCallbacks.add(new ExpirationCallback.Pre(days(7), days(30), days(1)) {
                                    @Override
                                    public void on(License license) {
                                        String general = LoggerMessageFormat.format(null, "\n" +
                                                "#\n" +
                                                "# License will expire on [{}]. If you have a new license, please update it.\n" +
                                                "# Otherwise, please reach out to your support contact.\n" +
                                                "# ", DATE_FORMATTER.printer().print(license.expiryDate()));
                                        if (!registeredLicensees.isEmpty()) {
                                            StringBuilder builder = new StringBuilder(general);
                                            builder.append(System.lineSeparator());
                                            builder.append("# Commercial plugins operate with reduced functionality on license " +
                                                    "expiration:");
                                            for (InternalLicensee licensee : registeredLicensees) {
                                                if (licensee.expirationMessages().length > 0) {
                                                    builder.append(System.lineSeparator());
                                                    builder.append("# - ");
                                                    builder.append(licensee.id());
                                                    for (String message : licensee.expirationMessages()) {
                                                        builder.append(System.lineSeparator());
                                                        builder.append("#  - ");
                                                        builder.append(message);
                                                    }
                                                }
                                            }
                                            logger.error("{}", builder);
                                        } else {
                                            logger.error("{}", general);
                                        }
                                    }
                                }
        );
        expirationCallbacks.add(new ExpirationCallback.Pre(days(0), days(7), TimeValue.timeValueMinutes(10)) {
                                    @Override
                                    public void on(License license) {
                                        String general = LoggerMessageFormat.format(null, "\n" +
                                                "#\n" +
                                                "# License will expire on [{}]. If you have a new license, please update it.\n" +
                                                "# Otherwise, please reach out to your support contact.\n" +
                                                "# ", DATE_FORMATTER.printer().print(license.expiryDate()));
                                        if (!registeredLicensees.isEmpty()) {
                                            StringBuilder builder = new StringBuilder(general);
                                            builder.append(System.lineSeparator());
                                            builder.append("# Commercial plugins operate with reduced functionality on license " +
                                                    "expiration:");
                                            for (InternalLicensee licensee : registeredLicensees) {
                                                if (licensee.expirationMessages().length > 0) {
                                                    builder.append(System.lineSeparator());
                                                    builder.append("# - ");
                                                    builder.append(licensee.id());
                                                    for (String message : licensee.expirationMessages()) {
                                                        builder.append(System.lineSeparator());
                                                        builder.append("#  - ");
                                                        builder.append(message);
                                                    }
                                                }
                                            }
                                            logger.error("{}", builder.toString());
                                        } else {
                                            logger.error("{}", general);
                                        }
                                    }
                                }
        );
        expirationCallbacks.add(new ExpirationCallback.Post(days(0), null, TimeValue.timeValueMinutes(10)) {
                                    @Override
                                    public void on(License license) {
                                        // logged when grace period begins
                                        String general = LoggerMessageFormat.format(null, "\n" +
                                                "#\n" +
                                                "# LICENSE EXPIRED ON [{}]. IF YOU HAVE A NEW LICENSE, PLEASE\n" +
                                                "# UPDATE IT. OTHERWISE, PLEASE REACH OUT TO YOUR SUPPORT CONTACT.\n" +
                                                "# ", DATE_FORMATTER.printer().print(license.expiryDate()));
                                        if (!registeredLicensees.isEmpty()) {
                                            StringBuilder builder = new StringBuilder(general);
                                            builder.append(System.lineSeparator());
                                            builder.append("# COMMERCIAL PLUGINS OPERATING WITH REDUCED FUNCTIONALITY");
                                            for (InternalLicensee licensee : registeredLicensees) {
                                                if (licensee.expirationMessages().length > 0) {
                                                    builder.append(System.lineSeparator());
                                                    builder.append("# - ");
                                                    builder.append(licensee.id());
                                                    for (String message : licensee.expirationMessages()) {
                                                        builder.append(System.lineSeparator());
                                                        builder.append("#  - ");
                                                        builder.append(message);
                                                    }
                                                }
                                            }
                                            logger.error("{}", builder.toString());
                                        } else {
                                            logger.error("{}", general);
                                        }
                                    }
                                }
        );
    }

    /**
     * Registers new license in the cluster
     * Master only operation. Installs a new license on the master provided it is VALID
     */
    public void registerLicense(final PutLicenseRequest request, final ActionListener<LicensesUpdateResponse> listener) {
        final License newLicense = request.license();
        final long now = System.currentTimeMillis();
        if (!verifyLicense(newLicense) || newLicense.issueDate() > now) {
            listener.onResponse(new LicensesUpdateResponse(true, LicensesStatus.INVALID));
        } else if (newLicense.expiryDate() < now) {
            listener.onResponse(new LicensesUpdateResponse(true, LicensesStatus.EXPIRED));
        } else {
            if (!request.acknowledged()) {
                final LicensesMetaData currentMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
                final License currentLicense = getLicense(currentMetaData);
                Map<String, String[]> acknowledgeMessages = new HashMap<>(registeredLicensees.size() + 1);
                if (currentLicense != null && !License.isAutoGeneratedLicense(currentLicense.signature()) // when current license is not
                        // an auto-generated license
                        && currentLicense.issueDate() > newLicense.issueDate()) { // and has a later issue date
                    acknowledgeMessages.put("license",
                            new String[]{"The new license is older than the currently installed license. Are you sure you want to " +
                                    "override the current license?"});
                }
                for (InternalLicensee licensee : registeredLicensees) {
                    String[] listenerAcknowledgeMessages = licensee.acknowledgmentMessages(currentLicense, newLicense);
                    if (listenerAcknowledgeMessages.length > 0) {
                        acknowledgeMessages.put(licensee.id(), listenerAcknowledgeMessages);
                    }
                }
                if (!acknowledgeMessages.isEmpty()) {
                    // needs acknowledgement
                    listener.onResponse(new LicensesUpdateResponse(false, LicensesStatus.VALID, ACKNOWLEDGEMENT_HEADER,
                            acknowledgeMessages));
                    return;
                }
            }
            clusterService.submitStateUpdateTask("register license [" + newLicense.uid() + "]", new
                    AckedClusterStateUpdateTask<LicensesUpdateResponse>(request, listener) {
                @Override
                protected LicensesUpdateResponse newResponse(boolean acknowledged) {
                    return new LicensesUpdateResponse(acknowledged, LicensesStatus.VALID);
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

    private boolean verifyLicense(final License license) {
        final byte[] publicKeyBytes;
        try (InputStream is = LicensesService.class.getResourceAsStream("/public.key")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            publicKeyBytes = out.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        return LicenseVerifier.verifyLicense(license, publicKeyBytes);
    }

    static TimeValue days(int days) {
        return TimeValue.timeValueHours(days * 24);
    }

    public static class LicensesUpdateResponse extends ClusterStateUpdateResponse {
        private final LicensesStatus status;
        private final String acknowledgementHeader;
        private final Map<String, String[]> acknowledgeMessages;

        public LicensesUpdateResponse(boolean acknowledged, LicensesStatus status) {
            this(acknowledged, status, null, Collections.<String, String[]>emptyMap());
        }

        public LicensesUpdateResponse(boolean acknowledged, LicensesStatus status, String acknowledgementHeader,
                                      Map<String, String[]> acknowledgeMessages) {
            super(acknowledged);
            this.status = status;
            this.acknowledgeMessages = acknowledgeMessages;
            this.acknowledgementHeader = acknowledgementHeader;
        }

        public LicensesStatus status() {
            return status;
        }

        public String acknowledgementHeader() {
            return acknowledgementHeader;
        }

        public Map<String, String[]> acknowledgeMessages() {
            return acknowledgeMessages;
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

    @Override
    public List<String> licenseesWithState(LicenseState state) {
        List<String> licensees = new ArrayList<>(registeredLicensees.size());
        for (InternalLicensee licensee : registeredLicensees) {
            if (licensee.currentLicenseState == state) {
                licensees.add(licensee.id());
            }
        }
        return licensees;
    }

    @Override
    public License getLicense() {
        final LicensesMetaData metaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        return getLicense(metaData);
    }

    /**
     * Master-only operation to generate a one-time global trial license.
     * The trial license is only generated and stored if the current cluster state metaData
     * has no signed/trial license
     */
    private void registerTrialLicense() {
        clusterService.submitStateUpdateTask("generate trial license for [" + trialLicenseDuration + "]", new ClusterStateUpdateTask() {
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
                    long issueDate = System.currentTimeMillis();
                    License.Builder specBuilder = License.builder()
                            .uid(UUID.randomUUID().toString())
                            .issuedTo(clusterService.state().getClusterName().value())
                            .maxNodes(trialLicenseMaxNodes)
                            .issueDate(issueDate)
                            .expiryDate(issueDate + trialLicenseDuration.getMillis());
                    License trialLicense = TrialLicense.create(specBuilder);
                    mdBuilder.putCustom(LicensesMetaData.TYPE, new LicensesMetaData(trialLicense));
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clusterService.add(this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        clusterService.remove(this);

        // cancel all notifications
        for (ScheduledFuture scheduledNotification : expiryNotifications) {
            FutureUtils.cancel(scheduledNotification);
        }
        for (ScheduledFuture eventNotification : eventNotifications) {
            FutureUtils.cancel(eventNotification);
        }

        // clear all handlers
        registeredLicensees.clear();

        // empty out notification queue
        expiryNotifications.clear();

        // clear current license
        currentLicense.set(null);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        transportService.removeHandler(REGISTER_TRIAL_LICENSE_ACTION_NAME);
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
            if (previousClusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                notifyAndSchedule(currentLicensesMetaData);
            } else {
                if (prevLicensesMetaData == null) {
                    if (currentLicensesMetaData != null) {
                        notifyAndSchedule(currentLicensesMetaData);
                    }
                } else if (!prevLicensesMetaData.equals(currentLicensesMetaData)) {
                    notifyAndSchedule(currentLicensesMetaData);
                }
            }
            // auto-generate license if no licenses ever existed
            // this will trigger a subsequent cluster changed event
            if (prevLicensesMetaData == null
                    && (currentLicensesMetaData == null || currentLicensesMetaData.getLicense() == null)) {
                requestTrialLicense(currentClusterState);
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("skipped license notifications reason: [{}]", GatewayService.STATE_NOT_RECOVERED_BLOCK);
        }
    }

    /**
     * Notifies registered licensees of license state change and/or new active license
     * based on the license in <code>currentLicensesMetaData</code>.
     * Additionally schedules license expiry notifications and event callbacks
     * relative to the current license's expiry
     */
    private void notifyAndSchedule(final LicensesMetaData currentLicensesMetaData) {
        final License license = getLicense(currentLicensesMetaData);
        if (license != null) {
            logger.debug("notifying [{}] listeners", registeredLicensees.size());
            long now = System.currentTimeMillis();
            if (license.issueDate() > now) {
                logger.info("license [{}] - invalid", license.uid());
                return;
            }
            long expiryDuration = license.expiryDate() - now;
            if (license.expiryDate() > now) {
                for (InternalLicensee licensee : registeredLicensees) {
                    licensee.onChange(license, LicenseState.ENABLED);
                }
                logger.info("license [{}] - valid", license.uid());
                final TimeValue delay = TimeValue.timeValueMillis(expiryDuration);
                // cancel any previous notifications
                cancelNotifications(expiryNotifications);
                try {
                    logger.debug("schedule grace notification after [{}] for license [{}]", delay.toString(), license.uid());
                    expiryNotifications.add(threadPool.schedule(delay, executorName(), new LicensingClientNotificationJob()));
                } catch (EsRejectedExecutionException ex) {
                    logger.debug("couldn't schedule grace notification", ex);
                }
            } else if ((license.expiryDate() + gracePeriodDuration.getMillis()) > now) {
                for (InternalLicensee licensee : registeredLicensees) {
                    licensee.onChange(license, LicenseState.GRACE_PERIOD);
                }
                logger.info("license [{}] - grace", license.uid());
                final TimeValue delay = TimeValue.timeValueMillis(expiryDuration + gracePeriodDuration.getMillis());
                // cancel any previous notifications
                cancelNotifications(expiryNotifications);
                try {
                    logger.debug("schedule expiry notification after [{}] for license [{}]", delay.toString(), license.uid());
                    expiryNotifications.add(threadPool.schedule(delay, executorName(), new LicensingClientNotificationJob()));
                } catch (EsRejectedExecutionException ex) {
                    logger.debug("couldn't schedule expiry notification", ex);
                }
            } else {
                for (InternalLicensee licensee : registeredLicensees) {
                    licensee.onChange(license, LicenseState.DISABLED);
                }
                logger.info("license [{}] - expired", license.uid());
            }
            if (!license.equals(currentLicense.get())) {
                currentLicense.set(license);
                // cancel all scheduled event notifications
                cancelNotifications(eventNotifications);
                // schedule expiry callbacks
                for (ExpirationCallback expirationCallback : this.expirationCallbacks) {
                    final TimeValue delay;
                    if (expirationCallback.matches(license.expiryDate(), now)) {
                        expirationCallback.on(license);
                        TimeValue frequency = expirationCallback.frequency();
                        delay = frequency != null ? frequency : expirationCallback.delay(expiryDuration);
                    } else {
                        delay = expirationCallback.delay(expiryDuration);
                    }
                    if (delay != null) {
                        eventNotifications.add(threadPool.schedule(delay, executorName(), new EventNotificationJob(expirationCallback)));
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("schedule [{}] after [{}]", expirationCallback, delay);
                    }
                }
                logger.debug("scheduled expiry callbacks for [{}] expiring after [{}]", license.uid(),
                        TimeValue.timeValueMillis(expiryDuration));
            }
        }
    }

    private class LicensingClientNotificationJob implements Runnable {
        @Override
        public void run() {
            logger.debug("running expiry notification");
            final ClusterState currentClusterState = clusterService.state();
            if (!currentClusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                final LicensesMetaData currentLicensesMetaData = currentClusterState.metaData().custom(LicensesMetaData.TYPE);
                notifyAndSchedule(currentLicensesMetaData);
            } else if (logger.isDebugEnabled()) {
                // next clusterChanged event will deal with the missed notifications
                logger.debug("skip expiry notification [{}]", GatewayService.STATE_NOT_RECOVERED_BLOCK);
            }
        }
    }

    private class EventNotificationJob implements Runnable {
        private final ExpirationCallback expirationCallback;

        EventNotificationJob(ExpirationCallback expirationCallback) {
            this.expirationCallback = expirationCallback;
        }

        @Override
        public void run() {
            logger.debug("running event notification for [{}]", expirationCallback);
            LicensesMetaData currentLicensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
            License license = getLicense(currentLicensesMetaData);
            if (license != null) {
                long now = System.currentTimeMillis();
                if (expirationCallback.matches(license.expiryDate(), now)) {
                    expirationCallback.on(license);
                    if (expirationCallback.frequency() != null) {
                        // schedule next event
                        eventNotifications.add(threadPool.schedule(expirationCallback.frequency(), executorName(), this));
                    }
                } else if (logger.isDebugEnabled()) {
                    logger.debug("skip scheduling notification for [{}] with license expiring after [{}]", expirationCallback,
                            TimeValue.timeValueMillis(license.expiryDate() - now));
                }
            }
            // clear out any finished event notifications
            while (!eventNotifications.isEmpty()) {
                ScheduledFuture notification = eventNotifications.peek();
                if (notification != null && notification.isDone()) {
                    // remove the notifications that are done
                    eventNotifications.poll();
                } else {
                    // stop emptying out the queue as soon as the first undone future hits
                    break;
                }
            }
        }
    }

    public static abstract class ExpirationCallback {

        public enum Orientation {PRE, POST}

        public static abstract class Pre extends ExpirationCallback {

            /**
             * Callback schedule prior to license expiry
             *
             * @param min       latest relative time to execute before license expiry
             * @param max       earliest relative time to execute before license expiry
             * @param frequency interval between execution
             */
            public Pre(TimeValue min, TimeValue max, TimeValue frequency) {
                super(Orientation.PRE, min, max, frequency);
            }

            @Override
            public boolean matches(long expirationDate, long now) {
                long expiryDuration = expirationDate - now;
                if (expiryDuration > 0L) {
                    if (expiryDuration <= max.getMillis()) {
                        return expiryDuration >= min.getMillis();
                    }
                }
                return false;
            }

            @Override
            public TimeValue delay(long expiryDuration) {
                return TimeValue.timeValueMillis(expiryDuration - max.getMillis());
            }
        }

        public static abstract class Post extends ExpirationCallback {

            /**
             * Callback schedule after license expiry
             *
             * @param min       earliest relative time to execute after license expiry
             * @param max       latest relative time to execute after license expiry
             * @param frequency interval between execution
             */
            public Post(TimeValue min, TimeValue max, TimeValue frequency) {
                super(Orientation.POST, min, max, frequency);
            }

            @Override
            public boolean matches(long expirationDate, long now) {
                long postExpiryDuration = now - expirationDate;
                if (postExpiryDuration > 0L) {
                    if (postExpiryDuration <= max.getMillis()) {
                        return postExpiryDuration >= min.getMillis();
                    }
                }
                return false;
            }

            @Override
            public TimeValue delay(long expiryDuration) {
                final long delay;
                if (expiryDuration >= 0L) {
                    delay = expiryDuration + min.getMillis();
                } else {
                    delay = (-1L * expiryDuration) - min.getMillis();
                }
                if (delay > 0L) {
                    return TimeValue.timeValueMillis(delay);
                } else {
                    return null;
                }
            }
        }

        protected final Orientation orientation;
        protected final TimeValue min;
        protected final TimeValue max;
        private final TimeValue frequency;

        private ExpirationCallback(Orientation orientation, TimeValue min, TimeValue max, TimeValue frequency) {
            this.orientation = orientation;
            this.min = (min == null) ? TimeValue.timeValueMillis(0) : min;
            this.max = (max == null) ? TimeValue.timeValueMillis(Long.MAX_VALUE) : max;
            this.frequency = frequency;
        }

        public TimeValue frequency() {
            return frequency;
        }

        public abstract TimeValue delay(long expiryDuration);

        public abstract boolean matches(long expirationDate, long now);

        public abstract void on(License license);

        @Override
        public String toString() {
            return LoggerMessageFormat.format(null, "ExpirationCallback:(orientation [{}],  min [{}], max [{}], freq [{}])",
                    orientation.name(), min, max, frequency);
        }
    }

    @Override
    public void register(Licensee licensee) {
        for (final InternalLicensee existingLicensee : registeredLicensees) {
            if (existingLicensee.id().equals(licensee.id())) {
                throw new IllegalStateException("listener: [" + licensee.id() + "] has been already registered");
            }
        }
        logger.debug("registering licensee [{}]", licensee.id());
        registeredLicensees.add(new InternalLicensee(licensee));
        final ClusterState clusterState = clusterService.state();
        if (clusterService.lifecycleState() == Lifecycle.State.STARTED
                && clusterState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false
                && clusterState.nodes().getMasterNode() != null) {
            final LicensesMetaData currentMetaData = clusterState.metaData().custom(LicensesMetaData.TYPE);
            if (currentMetaData == null || currentMetaData.getLicense() == null) {
                // triggers a cluster changed event
                // eventually notifying the current licensee
                requestTrialLicense(clusterState);
            } else {
                notifyAndSchedule(currentMetaData);
            }
        }
    }

    private void requestTrialLicense(final ClusterState currentState) {
        DiscoveryNode masterNode = currentState.nodes().getMasterNode();
        if (masterNode == null) {
            throw new IllegalStateException("master not available when registering auto-generated license");
        }
        transportService.sendRequest(masterNode,
                REGISTER_TRIAL_LICENSE_ACTION_NAME, TransportRequest.Empty.INSTANCE, EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    public License getLicense(final LicensesMetaData metaData) {
        if (metaData != null) {
            License license = metaData.getLicense();
            if (license != LicensesMetaData.LICENSE_TOMBSTONE) {
                boolean autoGeneratedLicense = License.isAutoGeneratedLicense(license.signature());
                if ((autoGeneratedLicense && TrialLicense.verify(license))
                        || (!autoGeneratedLicense && verifyLicense(license))) {
                    return license;
                }
            }
        }
        return null;
    }

    /**
     * Cancels out all notification futures
     */
    private static void cancelNotifications(Queue<ScheduledFuture> scheduledNotifications) {
        // clear out notification queue
        while (!scheduledNotifications.isEmpty()) {
            ScheduledFuture notification = scheduledNotifications.peek();
            if (notification != null) {
                // cancel
                FutureUtils.cancel(notification);
                scheduledNotifications.poll();
            }
        }
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    /**
     * Stores acknowledgement, expiration and license notification callbacks
     * for a registered listener
     */
    private class InternalLicensee {
        volatile License currentLicense = null;
        volatile LicenseState currentLicenseState = LicenseState.DISABLED;
        private final Licensee licensee;

        private InternalLicensee(Licensee licensee) {
            this.licensee = licensee;
        }

        @Override
        public String toString() {
            return "(listener: " + licensee.id() + ", state: " + currentLicenseState.name() + ")";
        }

        public String id() {
            return licensee.id();
        }

        public String[] expirationMessages() {
            return licensee.expirationMessages();
        }

        public String[] acknowledgmentMessages(License currentLicense, License newLicense) {
            return licensee.acknowledgmentMessages(currentLicense, newLicense);
        }

        public void onChange(License license, LicenseState state) {
            synchronized (this) {
                if (currentLicense == null // not yet initialized
                        || !currentLicense.equals(license)  // current license has changed
                        || currentLicenseState != state) { // same license but state has changed
                    logger.debug("licensee [{}] notified", licensee.id());
                    licensee.onChange(new Licensee.Status(license.operationMode(), state));
                    currentLicense = license;
                    currentLicenseState = state;
                }
            }
        }
    }

    /**
     * Request handler for trial license generation to master
     */
    private class RegisterTrialLicenseRequestHandler implements TransportRequestHandler<TransportRequest.Empty> {

        @Override
        public void messageReceived(TransportRequest.Empty empty, TransportChannel channel) throws Exception {
            registerTrialLicense();
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    // TODO - temporary hack for tests, should be removed once we introduce `ClockMock`
    public void setGracePeriodDuration(TimeValue gracePeriodDuration) {
        this.gracePeriodDuration = gracePeriodDuration;
    }

    // only for adding expiration callbacks for tests
    public void setExpirationCallbacks(List<ExpirationCallback> expirationCallbacks) {
        this.expirationCallbacks = expirationCallbacks;
    }

    // TODO - temporary hack for tests, should be removed once we introduce `ClockMock`
    public void setTrialLicenseDuration(TimeValue trialLicenseDuration) {
        this.trialLicenseDuration = trialLicenseDuration;
    }
}
