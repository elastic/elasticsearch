/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import net.nicholaswilliams.java.licensing.exception.ExpiredLicenseException;
import net.nicholaswilliams.java.licensing.exception.InvalidLicenseException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.license.core.ESLicenses.reduceAndMap;

/**
 * Service responsible for managing {@link org.elasticsearch.license.plugin.core.LicensesMetaData}
 * Interfaces through which this is exposed are:
 * - LicensesManagerService - responsible for managing signed and one-time-trial licenses
 * - LicensesClientService - responsible for feature registration and notification to consumer plugin(s)
 * <p/>
 */
@Singleton
public class LicensesService extends AbstractLifecycleComponent<LicensesService> implements ClusterStateListener, LicensesManagerService, LicensesClientService {

    public static final String REGISTER_TRIAL_LICENSE_ACTION_NAME = "internal:cluster/licenses/register_trial_license";

    private final ESLicenseManager licenseManager;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    /**
     * Currently active consumers to notify to
     */
    private final List<ListenerHolder> registeredListeners = new CopyOnWriteArrayList<>();

    /**
     * Currently pending consumers that has to be registered
     */
    private final Queue<ListenerHolder> pendingListeners = new ConcurrentLinkedQueue<>();

    /**
     * Currently active scheduledNotifications
     * All finished notifications will be cleared in {@link #clusterChanged(org.elasticsearch.cluster.ClusterChangedEvent)}
     * and {@link #scheduleNextNotification(long)}
     */
    private final Queue<ScheduledFuture> scheduledNotifications = new ConcurrentLinkedQueue<>();

    /**
     * The last licensesMetaData that has been notified by {@link #notifyFeatures(LicensesMetaData)}
     */
    private final AtomicReference<LicensesMetaData> lastObservedLicensesState;

    @Inject
    public LicensesService(Settings settings, ClusterService clusterService, ThreadPool threadPool, TransportService transportService, ESLicenseManager licenseManager) {
        super(settings);
        this.clusterService = clusterService;
        this.licenseManager = licenseManager;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.lastObservedLicensesState = new AtomicReference<>(null);
        if (DiscoveryNode.masterNode(settings)) {
            transportService.registerHandler(REGISTER_TRIAL_LICENSE_ACTION_NAME, new RegisterTrialLicenseRequestHandler());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerLicenses(final PutLicenseRequestHolder requestHolder, final ActionListener<LicensesUpdateResponse> listener) {
        final PutLicenseRequest request = requestHolder.request;
        final Set<ESLicense> newLicenses = Sets.newHashSet(request.licenses());
        LicensesStatus status = checkLicenses(newLicenses);
        if (status == LicensesStatus.VALID) {
            clusterService.submitStateUpdateTask(requestHolder.source, new AckedClusterStateUpdateTask<LicensesUpdateResponse>(request, listener) {
                @Override
                protected LicensesUpdateResponse newResponse(boolean acknowledged) {
                    return new LicensesUpdateResponse(acknowledged, LicensesStatus.VALID);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData metaData = currentState.metaData();
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);
                    final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicenses);
                    Set<String> newSignatures = licenseManager.toSignatures(newLicenses);
                    Set<String> newLicenseSignatures = Sets.union(licensesWrapper.signatures, newSignatures);
                    if (newLicenseSignatures.size() != licensesWrapper.signatures.size()) {
                        LicensesMetaData newLicensesMetaData = new LicensesMetaData(newLicenseSignatures, licensesWrapper.encodedTrialLicenses);
                        mdBuilder.putCustom(LicensesMetaData.TYPE, newLicensesMetaData);
                    } else {
                        mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicenses);
                    }
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }
            });
        } else {
            listener.onResponse(new LicensesUpdateResponse(true, status));
        }
    }

    public static class LicensesUpdateResponse extends ClusterStateUpdateResponse {
        private LicensesStatus status;

        public LicensesUpdateResponse(boolean acknowledged, LicensesStatus status) {
            super(acknowledged);
            this.status = status;
        }

        public LicensesStatus status() {
            return status;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeLicenses(final DeleteLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener) {
        final DeleteLicenseRequest request = requestHolder.request;
        clusterService.submitStateUpdateTask(requestHolder.source, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);
                final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicenses);

                Set<ESLicense> currentSignedLicenses = licensesWrapper.signedLicenses(licenseManager);
                Set<ESLicense> licensesToDelete = new HashSet<>();
                for (ESLicense license : currentSignedLicenses) {
                    if (request.features().contains(license.feature())) {
                        licensesToDelete.add(license);
                    }
                }
                if (!licensesToDelete.isEmpty()) {
                    Set<ESLicense> reducedLicenses = Sets.difference(currentSignedLicenses, licensesToDelete);
                    Set<String> newSignatures = licenseManager.toSignatures(reducedLicenses);
                    LicensesMetaData newLicensesMetaData = new LicensesMetaData(newSignatures, licensesWrapper.encodedTrialLicenses);
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    mdBuilder.putCustom(LicensesMetaData.TYPE, newLicensesMetaData);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                } else {
                    return currentState;
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> enabledFeatures() {
        Set<String> enabledFeatures = Sets.newHashSet();
        for (ListenerHolder holder : registeredListeners) {
            if (holder.enabled.get()) {
                enabledFeatures.add(holder.feature);
            }
        }
        return enabledFeatures;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ESLicense> getLicenses() {
        LicensesMetaData currentMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        if (currentMetaData != null) {
            // don't use ESLicenses.reduceAndMap, as it will merge out expired licenses
            Set<ESLicense> licenses = Sets.union(licenseManager.fromSignatures(currentMetaData.getSignatures()),
                    TrialLicenseUtils.fromEncodedTrialLicenses(currentMetaData.getEncodedTrialLicenses()));

            // bucket license for feature with the latest expiry date
            Map<String, ESLicense> licenseMap = new HashMap<>();
            for (ESLicense license : licenses) {
                if (!licenseMap.containsKey(license.feature())) {
                    licenseMap.put(license.feature(), license);
                } else {
                    ESLicense prevLicense = licenseMap.get(license.feature());
                    if (license.expiryDate() > prevLicense.expiryDate()) {
                        licenseMap.put(license.feature(), license);
                    }
                }
            }

            // sort the licenses by issue date
            List<ESLicense> reducedLicenses = new ArrayList<>(licenseMap.values());
            Collections.sort(reducedLicenses, new Comparator<ESLicense>() {
                @Override
                public int compare(ESLicense license1, ESLicense license2) {
                    return (int) (license2.issueDate() - license1.issueDate());
                }
            });
            return reducedLicenses;
        }
        return Collections.emptyList();
    }

    private LicensesStatus checkLicenses(Set<ESLicense> licenses) {
        final ImmutableMap<String, ESLicense> map = reduceAndMap(licenses);
        return checkLicenses(map);
    }

    private LicensesStatus checkLicenses(Map<String, ESLicense> licenseMap) {
        LicensesStatus status = LicensesStatus.VALID;
        try {
            licenseManager.verifyLicenses(licenseMap);
        } catch (ExpiredLicenseException e) {
            status = LicensesStatus.EXPIRED;
        } catch (InvalidLicenseException e) {
            status = LicensesStatus.INVALID;
        }
        return status;
    }

    /**
     * Master-only operation to generate a one-time trial license for a feature.
     * The trial license is only generated and stored if the current cluster state metaData
     * has no signed/one-time-trial license for the feature in question
     */
    private void registerTrialLicense(final RegisterTrialLicenseRequest request) {
        clusterService.submitStateUpdateTask("register trial license []", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Change to debug
                logger.info("Processed Trial License registration");
                LicensesMetaData licensesMetaData = newState.metaData().custom(LicensesMetaData.TYPE);
                logLicenseMetaDataStats("new", licensesMetaData);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                LicensesMetaData currentLicensesMetaData = metaData.custom(LicensesMetaData.TYPE);
                final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicensesMetaData);
                // do not generate a trial license for a feature that already has a signed/trial license
                if (checkTrialLicenseGenerationCondition(request.feature, licensesWrapper)) {
                    Set<String> newTrialLicenses = Sets.union(licensesWrapper.encodedTrialLicenses,
                            Sets.newHashSet(generateEncodedTrialLicense(request.feature, request.duration, request.maxNodes)));
                    final LicensesMetaData newLicensesMetaData = new LicensesMetaData(
                            licensesWrapper.signatures, newTrialLicenses);
                    mdBuilder.putCustom(LicensesMetaData.TYPE, newLicensesMetaData);
                } else {
                    mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicensesMetaData);
                }
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.info("LicensesService: " + source, t);
            }

            private boolean checkTrialLicenseGenerationCondition(String feature, LicensesWrapper licensesWrapper) {
                for (ESLicense license : Sets.union(licensesWrapper.signedLicenses(licenseManager),
                        licensesWrapper.trialLicenses())) {
                    if (license.feature().equals(feature)) {
                        return false;
                    }
                }
                return true;
            }

            private String generateEncodedTrialLicense(String feature, TimeValue duration, int maxNodes) {
                return TrialLicenseUtils.toEncodedTrialLicense(
                        TrialLicenseUtils.builder()
                                .issuedTo(clusterService.state().getClusterName().value())
                                .issueDate(System.currentTimeMillis())
                                .duration(duration)
                                .feature(feature)
                                .maxNodes(maxNodes)
                                .build()
                );
            }
        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        //Change to debug
        logger.info("Started LicensesService");
        if (DiscoveryNode.dataNode(settings) || DiscoveryNode.masterNode(settings)) {
            clusterService.add(this);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        clusterService.remove(this);

        // cancel all notifications
        for (ScheduledFuture scheduledNotification : scheduledNotifications) {
            scheduledNotification.cancel(true);
        }

        // notify features to be disabled
        for (ListenerHolder holder : registeredListeners) {
            holder.disableFeatureIfNeeded();
        }
        // clear all handlers
        registeredListeners.clear();

        // empty out notification queue
        scheduledNotifications.clear();

        lastObservedLicensesState.set(null);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("Closing LicensesService");
        transportService.removeHandler(REGISTER_TRIAL_LICENSE_ACTION_NAME);
    }

    /**
     * When there is no global block on {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK}:
     *  - tries to register any {@link #pendingListeners} by calling {@link #registeredListeners}
     *  - if any {@link #pendingListeners} are registered successfully or if previous cluster state had a block on
     *      {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK}, calls
     *      {@link #notifyFeaturesAndScheduleNotification(LicensesMetaData)}
     *  - else calls {@link #notifyFeaturesAndScheduleNotificationIfNeeded(LicensesMetaData)}
     *
     * clears up any finished notifications on every call
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        clearFinishedNotifications();
        if (!event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            LicensesMetaData oldLicensesMetaData = event.previousState().getMetaData().custom(LicensesMetaData.TYPE);
            LicensesMetaData currentLicensesMetaData = event.state().getMetaData().custom(LicensesMetaData.TYPE);
            logLicenseMetaDataStats("old", oldLicensesMetaData);
            logLicenseMetaDataStats("new", currentLicensesMetaData);

            // Check pending feature registrations and try to complete registrations
            boolean addedNewRegisteredListener = false;
            if (!pendingListeners.isEmpty()) {
                ListenerHolder pendingRegistrationLister;
                while ((pendingRegistrationLister = pendingListeners.poll()) != null) {
                    boolean masterAvailable = registerListener(pendingRegistrationLister);
                    logger.info("trying to register pending listener for " + pendingRegistrationLister.feature + " masterAvailable: " + masterAvailable);
                    if (!masterAvailable) {
                        // if the master is not available do not, break out of trying pendingListeners
                        pendingListeners.add(pendingRegistrationLister);
                        break;
                    } else {
                        logger.info("successfully registered listener for: " + pendingRegistrationLister.feature);
                        registeredListeners.add(pendingRegistrationLister);
                        // make sure to notify new registered feature
                        // notifications could have been scheduled for it before it was registered
                        addedNewRegisteredListener = true;
                    }
                }
            }

            // notify all interested plugins
            // Change to debug
            if (event.previousState().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) || addedNewRegisteredListener) {
                logger.info("calling notifyFeaturesAndScheduleNotification from clusterChanged");
                notifyFeaturesAndScheduleNotification(currentLicensesMetaData);
            } else {
                logger.info("calling notifyFeaturesAndScheduleNotificationIfNeeded from clusterChanged");
                notifyFeaturesAndScheduleNotificationIfNeeded(currentLicensesMetaData);
            }
        } else {
            logger.info("clusterChanged: no action [has STATE_NOT_RECOVERED_BLOCK]");
        }
    }

    /**
     * Calls {@link #notifyFeaturesAndScheduleNotification(LicensesMetaData)} with <code>currentLicensesMetaData</code>
     * if it was not already notified on
     */
    private void notifyFeaturesAndScheduleNotificationIfNeeded(LicensesMetaData currentLicensesMetaData) {
        final LicensesMetaData lastNotifiedLicensesMetaData = lastObservedLicensesState.get();
        if (lastNotifiedLicensesMetaData != null && lastNotifiedLicensesMetaData.equals(currentLicensesMetaData)) {
            logger.info("currentLicensesMetaData has been already notified on");
            return;
        }
        notifyFeaturesAndScheduleNotification(currentLicensesMetaData);
    }

    /**
     * Calls {@link #notifyFeatures(LicensesMetaData)} with <code>currentLicensesMetaData</code>
     * if needed, also schedules the earliest expiry notification for registered feature(s)
     */
    private void notifyFeaturesAndScheduleNotification(LicensesMetaData currentLicensesMetaData) {
        long nextScheduleFrequency = notifyFeatures(currentLicensesMetaData);
        if (nextScheduleFrequency != -1l) {
            scheduleNextNotification(nextScheduleFrequency);
        }
    }

    /**
     * Checks license expiry for all the registered feature(s), upon completion
     * sets <code>currentLicensesMetaData</code> to {@link #lastObservedLicensesState}
     * to ensure the same license(s) are not notified on from
     * {@link #clusterChanged(org.elasticsearch.cluster.ClusterChangedEvent)}
     *
     * @return -1 if there are no expiring license(s) for any registered feature(s), else
     * returns the minimum of the expiry times of all the registered feature(s) to
     * schedule an expiry notification
     */
    private long notifyFeatures(LicensesMetaData currentLicensesMetaData) {
        long nextScheduleFrequency = -1l;
        long offset = TimeValue.timeValueMillis(100).getMillis();
        StringBuilder sb = new StringBuilder("Registered listeners: [ ");
        for (ListenerHolder listenerHolder : registeredListeners) {

            sb.append("( ");
            sb.append("feature:");
            sb.append(listenerHolder.feature);
            sb.append(", ");

            long expiryDate;
            if ((expiryDate = expiryDateForFeature(listenerHolder.feature, currentLicensesMetaData)) != -1l) {
                sb.append(" license expiry: ");
                sb.append(expiryDate);
                sb.append(", ");
            }
            long expiryDuration = expiryDate - System.currentTimeMillis();

            if (expiryDate == -1l) {
                sb.append("no trial/signed license found");
                sb.append(", ");
            } else {
                sb.append("license expires in: ");
                sb.append(TimeValue.timeValueMillis(expiryDuration).toString());
                sb.append(", ");
            }

            if (expiryDuration > 0l) {
                sb.append("calling enableFeatureIfNeeded");

                listenerHolder.enableFeatureIfNeeded();
                if (nextScheduleFrequency == -1l) {
                    nextScheduleFrequency = expiryDuration + offset;
                } else {
                    nextScheduleFrequency = Math.min(expiryDuration + offset, nextScheduleFrequency);
                }
            } else {
                sb.append("calling disableFeatureIfNeeded");
                listenerHolder.disableFeatureIfNeeded();
            }
            sb.append(" )");
        }
        sb.append("]");
        // Change to debug
        logger.info(sb.toString());

        logLicenseMetaDataStats("Setting last observed metaData", currentLicensesMetaData);
        lastObservedLicensesState.set(currentLicensesMetaData);

        if (nextScheduleFrequency == -1l) {
            logger.info("no need to schedule next notification");
        } else {
            logger.info("next notification time: " + TimeValue.timeValueMillis(nextScheduleFrequency).toString());
        }

        return nextScheduleFrequency;

    }
    private void logLicenseMetaDataStats(String prefix, LicensesMetaData licensesMetaData) {
        if (licensesMetaData != null) {
            logger.info(prefix + " LicensesMetaData: signedLicenses: " + licensesMetaData.getSignatures().size() + " trialLicenses: " + licensesMetaData.getEncodedTrialLicenses().size());
        } else {
            logger.info(prefix + " LicensesMetaData: signedLicenses: 0 trialLicenses: 0");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(String feature, TrialLicenseOptions trialLicenseOptions, Listener listener) {
        final ListenerHolder listenerHolder = new ListenerHolder(feature, trialLicenseOptions, listener);
        if (registerListener(listenerHolder)) {
            logger.info("successfully registered listener for: " + listenerHolder.feature);
            registeredListeners.add(listenerHolder);
        } else {
            logger.info("add listener for: " + listenerHolder.feature + " to pending registration queue");
            pendingListeners.add(listenerHolder);
        }
    }

    /**
     * Notifies new feature listener if it already has a signed license
     * if new feature has a non-null trial license option, a master node request is made to generate the trial license
     * if no trial license option is specified for the feature and no signed license is found,
     * then notifies features to be disabled
     *
     * @param listenerHolder of the feature to register
     * @return true if registration has been completed, false otherwise (if masterNode is not available & trail license spec is provided
     *  or if there is a global block on {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK})
     */
    private boolean registerListener(final ListenerHolder listenerHolder) {
        logger.info("Registering listener for " + listenerHolder.feature);
        if (clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.info("Store as pendingRegistration [cluster has NOT_RECOVERED_BLOCK]");
            return false;
        }

        LicensesMetaData currentMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        if (expiryDateForFeature(listenerHolder.feature, currentMetaData) == -1l) {
            // does not have any license so generate a trial license
            TrialLicenseOptions options = listenerHolder.trialLicenseOptions;
            if (options != null) {
                // Trial license option is provided
                RegisterTrialLicenseRequest request = new RegisterTrialLicenseRequest(listenerHolder.feature,
                        options.duration, options.maxNodes);
                if (clusterService.state().nodes().localNodeMaster()) {
                    logger.info("Executing trial license request");
                    registerTrialLicense(request);
                } else {
                    DiscoveryNode masterNode = clusterService.state().nodes().masterNode();
                    if (masterNode != null) {
                        logger.info("Sending trial license request to master");
                        transportService.sendRequest(masterNode,
                                REGISTER_TRIAL_LICENSE_ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
                    } else {
                        // could not sent register trial license request to master
                        logger.info("Store as pendingRegistration [master not available yet]");
                        return false;
                    }
                }
            } else {
                // notify feature as clusterChangedEvent may not happen
                // as no trial or signed license has been found for feature
                // Change to debug
                logger.info("Calling notifyFeaturesAndScheduleNotification [no trial license spec provided]");
                notifyFeaturesAndScheduleNotification(currentMetaData);
            }
        } else {
            // signed license already found for the new registered
            // feature, notify feature on registration
            logger.info("Calling notifyFeaturesAndScheduleNotification [signed/trial license available]");
            notifyFeaturesAndScheduleNotification(currentMetaData);
        }
        return true;
    }

    private long expiryDateForFeature(String feature, LicensesMetaData currentLicensesMetaData) {
        final Map<String, ESLicense> effectiveLicenses = getEffectiveLicenses(currentLicensesMetaData);
        ESLicense featureLicense;
        if ((featureLicense = effectiveLicenses.get(feature)) != null) {
            logger.info("effective license for " + feature + " relative expiry: " +
                    TimeValue.timeValueMillis(effectiveLicenses.get(feature).expiryDate() - System.currentTimeMillis()));
            return featureLicense.expiryDate();
        }
        logger.info("no effective license for " + feature);
        return -1l;
    }

    private Map<String, ESLicense> getEffectiveLicenses(LicensesMetaData metaData) {
        Map<String, ESLicense> map = new HashMap<>();
        if (metaData != null) {
            Set<ESLicense> esLicenses = new HashSet<>();
            esLicenses.addAll(licenseManager.fromSignatures(metaData.getSignatures()));
            esLicenses.addAll(TrialLicenseUtils.fromEncodedTrialLicenses(metaData.getEncodedTrialLicenses()));
            return reduceAndMap(esLicenses);
        }
        return ImmutableMap.copyOf(map);

    }

    /**
     * Clears out any completed notification future from
     * {@link #scheduledNotifications}
     */
    private void clearFinishedNotifications() {
        while (!scheduledNotifications.isEmpty()) {
            ScheduledFuture notification = scheduledNotifications.peek();
            if (notification.isDone()) {
                // remove the notifications that are done
                scheduledNotifications.poll();
            } else {
                // stop emptying out the queue as soon as the first undone future hits
                break;
            }
        }
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    /**
     * Schedules an expiry notification with a delay of <code>nextScheduleDelay</code>
     */
    private void scheduleNextNotification(long nextScheduleDelay) {
        clearFinishedNotifications();

        try {
            final TimeValue delay = TimeValue.timeValueMillis(nextScheduleDelay);
            scheduledNotifications.add(threadPool.schedule(delay, executorName(), new LicensingClientNotificationJob()));
            logger.info("Scheduling next notification after: " + delay);
        } catch (EsRejectedExecutionException ex) {
            logger.info("Couldn't re-schedule licensing client notification job", ex);
        }
    }

    /**
     * Job for notifying on expired license(s) to registered feature(s)
     * In case of a global block on {@link org.elasticsearch.gateway.GatewayService#STATE_NOT_RECOVERED_BLOCK},
     * the notification is not run, instead the feature(s) would be notified on the next
     * {@link #clusterChanged(org.elasticsearch.cluster.ClusterChangedEvent)} with no global block
     */
    public class LicensingClientNotificationJob implements Runnable {

        public LicensingClientNotificationJob() {}

        @Override
        public void run() {
            if (logger.isTraceEnabled()) {
                logger.trace("Performing LicensingClientNotificationJob");
            }

            // next clusterChanged event will deal with the missed notifications
            if (!clusterService.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                LicensesMetaData currentLicensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
                long nextScheduleDelay = notifyFeatures(currentLicensesMetaData);
                if (nextScheduleDelay != -1l) {
                    try {
                        scheduleNextNotification(nextScheduleDelay);
                    } catch (EsRejectedExecutionException ex) {
                        logger.info("Reschedule licensing client notification job was rejected", ex);
                    }
                }
            }
        }
    }


    public static class PutLicenseRequestHolder {
        private final PutLicenseRequest request;
        private final String source;

        public PutLicenseRequestHolder(PutLicenseRequest request, String source) {
            this.request = request;
            this.source = source;
        }
    }

    public static class DeleteLicenseRequestHolder {
        private final DeleteLicenseRequest request;
        private final String source;

        public DeleteLicenseRequestHolder(DeleteLicenseRequest request, String source) {
            this.request = request;
            this.source = source;
        }
    }

    public static class TrialLicenseOptions {
        final TimeValue duration;
        final int maxNodes;

        public TrialLicenseOptions(TimeValue duration, int maxNodes) {
            this.duration = duration;
            this.maxNodes = maxNodes;
        }
    }

    /**
     * Stores configuration and listener for a feature
     */
    private class ListenerHolder {
        final String feature;
        final TrialLicenseOptions trialLicenseOptions;
        final Listener listener;

        final AtomicBoolean enabled = new AtomicBoolean(false); // by default, a consumer plugin should be disabled

        private ListenerHolder(String feature, TrialLicenseOptions trialLicenseOptions, Listener listener) {
            this.feature = feature;
            this.trialLicenseOptions = trialLicenseOptions;
            this.listener = listener;
        }

        private void enableFeatureIfNeeded() {
            if (enabled.compareAndSet(false, true)) {
                listener.onEnabled();
            }
        }

        private void disableFeatureIfNeeded() {
            if (enabled.compareAndSet(true, false)) {
                listener.onDisabled();
            }
        }
    }

    /**
     * Thin wrapper to work with {@link org.elasticsearch.license.plugin.core.LicensesMetaData}
     * Never mutates the wrapped metaData
     */
    private static class LicensesWrapper {

        public static LicensesWrapper wrap(LicensesMetaData licensesMetaData) {
            return new LicensesWrapper(licensesMetaData);
        }

        private ImmutableSet<String> signatures = ImmutableSet.of();
        private ImmutableSet<String> encodedTrialLicenses = ImmutableSet.of();

        private LicensesWrapper(LicensesMetaData licensesMetaData) {
            if (licensesMetaData != null) {
                this.signatures = ImmutableSet.copyOf(licensesMetaData.getSignatures());
                this.encodedTrialLicenses = ImmutableSet.copyOf(licensesMetaData.getEncodedTrialLicenses());
            }
        }

        public Set<ESLicense> signedLicenses(ESLicenseManager licenseManager) {
            return licenseManager.fromSignatures(signatures);
        }

        public Set<ESLicense> trialLicenses() {
            return TrialLicenseUtils.fromEncodedTrialLicenses(encodedTrialLicenses);
        }
    }

    /**
     * Request for trial license generation to master
     */
    private static class RegisterTrialLicenseRequest extends TransportRequest {
        private int maxNodes;
        private String feature;
        private TimeValue duration;

        private RegisterTrialLicenseRequest() {}

        private RegisterTrialLicenseRequest(String feature, TimeValue duration, int maxNodes) {
            this.maxNodes = maxNodes;
            this.feature = feature;
            this.duration = duration;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            maxNodes = in.readVInt();
            feature = in.readString();
            duration = new TimeValue(in.readVLong(), TimeUnit.MILLISECONDS);
        }


        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(maxNodes);
            out.writeString(feature);
            out.writeVLong(duration.getMillis());
        }
    }

    /**
     * Request handler for trial license generation to master
     */
    private class RegisterTrialLicenseRequestHandler extends BaseTransportRequestHandler<RegisterTrialLicenseRequest> {
        @Override
        public RegisterTrialLicenseRequest newInstance() {
            return new RegisterTrialLicenseRequest();
        }

        @Override
        public void messageReceived(RegisterTrialLicenseRequest request, TransportChannel channel) throws Exception {
            registerTrialLicense(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
