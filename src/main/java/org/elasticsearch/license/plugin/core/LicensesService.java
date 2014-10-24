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
import org.elasticsearch.license.plugin.core.trial.TrialLicenseUtils;
import org.elasticsearch.license.plugin.core.trial.TrialLicenses;
import org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.license.manager.Utils.reduceAndMap;
import static org.elasticsearch.license.plugin.core.trial.TrialLicenses.TrialLicense;

/**
 * Service responsible for managing {@link org.elasticsearch.license.plugin.core.LicensesMetaData}
 * Interfaces through which this is exposed are:
 * - LicensesManagerService - responsible for adding/deleting signed licenses
 * - LicensesClientService - allow interested plugins (features) to register to licensing notifications
 *
 * TODO: documentation
 * TODO: figure out when to check GatewayService.STATE_NOT_RECOVERED_BLOCK
 */
@Singleton
public class LicensesService extends AbstractLifecycleComponent<LicensesService> implements ClusterStateListener, LicensesManagerService, LicensesClientService {

    public static final String REGISTER_TRIAL_LICENSE_ACTION_NAME = "internal:cluster/licenses/register_trial_license";

    private final ESLicenseManager esLicenseManager;

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private List<ListenerHolder> registeredListeners = new CopyOnWriteArrayList<>();

    private Queue<ListenerHolder> pendingRegistrations = new ConcurrentLinkedQueue<>();

    private volatile ScheduledFuture notificationScheduler;

    @Inject
    public LicensesService(Settings settings, ClusterService clusterService, ThreadPool threadPool, TransportService transportService, ESLicenseManager esLicenseManager) {
        super(settings);
        this.clusterService = clusterService;
        this.esLicenseManager = esLicenseManager;
        this.threadPool = threadPool;
        this.transportService = transportService;
        transportService.registerHandler(REGISTER_TRIAL_LICENSE_ACTION_NAME, new RegisterTrialLicenseRequestHandler());
    }

    /**
     * Registers new licenses in the cluster
     * <p/>
     * This method can be only called on the master node. It tries to create a new licenses on the master
     * and if provided license(s) is VALID it is added to cluster metadata.
     *
     * @return LicensesStatus indicating if the provided license(s) is VALID (accepted), INVALID (tampered license) or EXPIRED
     */
    @Override
    public LicensesStatus registerLicenses(final PutLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener) {
        final PutLicenseRequest request = requestHolder.request;
        final Set<ESLicense> newLicenses = Sets.newHashSet(request.licenses());
        LicensesStatus status = checkLicenses(newLicenses);
        switch (status) {
            case VALID:
                break;
            case INVALID:
            case EXPIRED:
                return status;
        }
        clusterService.submitStateUpdateTask(requestHolder.source, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);
                final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicenses);
                licensesWrapper.addSignedLicenses(esLicenseManager, Sets.newHashSet(newLicenses));
                mdBuilder.putCustom(LicensesMetaData.TYPE, licensesWrapper.createLicensesMetaData());
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

        });
        return LicensesStatus.VALID;
    }

    @Override
    public void unregisterLicenses(final DeleteLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener) {
        final DeleteLicenseRequest request = requestHolder.request;
        clusterService.submitStateUpdateTask(requestHolder.source, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);
                final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicenses);
                licensesWrapper.removeFeatures(esLicenseManager, request.features());
                mdBuilder.putCustom(LicensesMetaData.TYPE, licensesWrapper.createLicensesMetaData());
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }
        });
    }

    @Override
    public LicensesStatus checkLicenses(Set<ESLicense> licenses) {
        final ImmutableMap<String, ESLicense> map = reduceAndMap(licenses);
        return checkLicenses(map);
    }

    private LicensesStatus checkLicenses(Map<String, ESLicense> licenseMap) {
        LicensesStatus status = LicensesStatus.VALID;
        try {
            esLicenseManager.verifyLicenses(licenseMap);
        } catch (ExpiredLicenseException e) {
            status = LicensesStatus.EXPIRED;
        } catch (InvalidLicenseException e) {
            status = LicensesStatus.INVALID;
        }
        return status;
    }

    @Override
    public Set<String> enabledFeatures() {
        Set<String> enabledFeatures = Sets.newHashSet();
        if (registeredListeners != null) {
            for (ListenerHolder holder : registeredListeners) {
                if (holder.enabled.get()) {
                    enabledFeatures.add(holder.feature);
                }
            }
        }
        return enabledFeatures;
    }

    @Override
    public List<ESLicense> getLicenses() {
        LicensesMetaData currentMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        Set<ESLicense> trialLicenses = new HashSet<>();
        if (currentMetaData != null) {
            Set<ESLicense> currentLicenses = esLicenseManager.fromSignatures(currentMetaData.getSignatures());
            TrialLicenses currentTrialLicenses = TrialLicenseUtils.fromEncodedTrialLicenses(currentMetaData.getEncodedTrialLicenses());
            for (TrialLicense trialLicense : currentTrialLicenses) {
                trialLicenses.add(ESLicense.builder()
                                .uid(trialLicense.uid())
                                .issuedTo(trialLicense.issuedTo())
                                .issueDate(trialLicense.issueDate())
                                .type(ESLicense.Type.TRIAL)
                                .subscriptionType(ESLicense.SubscriptionType.NONE)
                                .feature(trialLicense.feature())
                                .maxNodes(trialLicense.maxNodes())
                                .expiryDate(trialLicense.expiryDate())
                                .issuer("elasticsearch").buildInternal()
                );
            }
            Set<ESLicense> licenses = Sets.union(currentLicenses, trialLicenses);

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


    private void registerTrialLicense(final RegisterTrialLicenseRequest request) {
        clusterService.submitStateUpdateTask("register trial license []", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Change to debug
                logger.info("Processed Trial License registration");
                LicensesMetaData licensesMetaData = newState.metaData().custom(LicensesMetaData.TYPE);
                if (licensesMetaData != null) {
                    logger.info("New state: signedLicenses: " + licensesMetaData.getSignatures().size() + " trialLicenses: " + licensesMetaData.getEncodedTrialLicenses().size());
                }
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                LicensesMetaData currentLicensesMetaData = metaData.custom(LicensesMetaData.TYPE);
                final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicensesMetaData);
                // do not generate a trial license for a feature that already has a signed license
                if (!hasLicenseForFeature(request.feature, currentLicensesMetaData)) {
                    licensesWrapper.addTrialLicense(generateTrialLicense(request.feature, request.duration, request.maxNodes));
                }
                mdBuilder.putCustom(LicensesMetaData.TYPE, licensesWrapper.createLicensesMetaData());
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.info("LicensesService: " + source, t);
            }

            private TrialLicense generateTrialLicense(String feature, TimeValue duration, int maxNodes) {
                return TrialLicensesBuilder.trialLicenseBuilder()
                        .issuedTo(clusterService.state().getClusterName().value())
                        .issueDate(System.currentTimeMillis())
                        .duration(duration)
                        .feature(feature)
                        .maxNodes(maxNodes)
                        .build();
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
        // Should notificationScheduler be cancelled on stop as well?
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        logger.info("Closing LicensesService");
        if (notificationScheduler != null) {
            notificationScheduler.cancel(true);
            notificationScheduler = null;
        }
        clusterService.remove(this);

        if (registeredListeners != null) {
            // notify features to be disabled
            for (ListenerHolder holder : registeredListeners) {
                holder.disableFeatureIfNeeded();
            }
            // clear all handlers
            registeredListeners.clear();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {

            if (!pendingRegistrations.isEmpty()) {
                ListenerHolder pendingRegistrationLister;
                while ((pendingRegistrationLister = pendingRegistrations.poll()) != null) {
                    boolean masterAvailable = registerListener(pendingRegistrationLister);
                    logger.info("trying to register pending listener for " + pendingRegistrationLister.feature + " masterAvailable: " + masterAvailable);
                    if (!masterAvailable) {
                        // if the master is not available do not, break out of trying pendingRegistrations
                        break;
                    }
                }
            }

            // notify all interested plugins
            logger.info("LicensesService: cluster state changed");
            if (checkIfUpdatedMetaData(event)) {
                final LicensesMetaData currentLicensesMetaData = event.state().getMetaData().custom(LicensesMetaData.TYPE);
                // Change to debug
                if (currentLicensesMetaData != null) {
                    logger.info("LicensesMetaData: signedLicenses: " + currentLicensesMetaData.getSignatures().size() + " trialLicenses: " + currentLicensesMetaData.getEncodedTrialLicenses().size());
                } else {
                    logger.info("LicensesMetaData: signedLicenses: 0 trialLicenses: 0");
                }

                // Change to debug
                logger.info("calling notifyFeatures from clusterChanged");
                long nextScheduleFrequency = notifyFeatures(currentLicensesMetaData);
                if (notificationScheduler == null) {
                    notificationScheduler = threadPool.schedule(TimeValue.timeValueMillis(nextScheduleFrequency), executorName(),
                            new SubmitReschedulingLicensingClientNotificationJob());
                }
            }
        }
    }

    private boolean checkIfUpdatedMetaData(ClusterChangedEvent event) {
        LicensesMetaData oldMetaData = event.previousState().getMetaData().custom(LicensesMetaData.TYPE);
        LicensesMetaData newMetaData = event.state().getMetaData().custom(LicensesMetaData.TYPE);
        return !((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equals(newMetaData)));
    }


    @Override
    public void register(String feature, TrialLicenseOptions trialLicenseOptions, Listener listener) {
        final ListenerHolder listenerHolder = new ListenerHolder(feature, trialLicenseOptions, listener);
        registeredListeners.add(listenerHolder);
        registerListener(listenerHolder);
    }

    /**
     * Notifies new feature listener if it already has a signed license
     * if new feature has a non-null trial license option, a master node request is made to generate the trial license
     * if no trial license option is specified for the feature and no signed license is found,
     * then notifies features to be disabled
     *
     * @param listenerHolder of the feature to register
     *
     * @return true if registration has been completed, false otherwise (if masterNode is not available)
     */
    private boolean registerListener(final ListenerHolder listenerHolder) {
        logger.info("Registering listener for " + listenerHolder.feature);

        LicensesMetaData currentMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
        if (!hasLicenseForFeature(listenerHolder.feature, currentMetaData)) {
            // does not have actual license so generate a trial license
            TrialLicenseOptions options = listenerHolder.trialLicenseOptions;
            if (options != null) {
                // Trial license option is provided
                RegisterTrialLicenseRequest request = new RegisterTrialLicenseRequest(listenerHolder.feature,
                        new TimeValue(options.durationInDays, TimeUnit.DAYS), options.maxNodes);
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
                        pendingRegistrations.add(listenerHolder);
                        return false;
                    }
                }
            } else {
                // notify feature as clusterChangedEvent may not happen
                // as no trial or signed license has been found for feature
                // Change to debug
                logger.info("Calling notifyFeatures [no trial license spec provided]");
                notifyFeatures(currentMetaData);
            }
        } else {
            // signed license already found for the new registered
            // feature, notify feature on registration
            logger.info("Calling notifyFeatures [signed license available]");
            notifyFeatures(currentMetaData);
        }
        return true;
    }

    private boolean hasLicenseForFeature(String feature, LicensesMetaData currentLicensesMetaData) {
        return esLicenseManager.hasLicenseForFeature(feature, getEffectiveLicenses(currentLicensesMetaData));
    }


    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    public Map<String, ESLicense> getEffectiveLicenses(LicensesMetaData metaData) {
        Map<String, ESLicense> map = new HashMap<>();
        if (metaData != null) {
            Set<ESLicense> esLicenses = new HashSet<>();
            for (String signature : metaData.getSignatures()) {
                esLicenses.add(esLicenseManager.fromSignature(signature));
            }
            return reduceAndMap(esLicenses);
        }
        return ImmutableMap.copyOf(map);

    }

    public class SubmitReschedulingLicensingClientNotificationJob implements Runnable {
        @Override
        public void run() {
            if (logger.isTraceEnabled()) {
                logger.trace("Submitting new rescheduling licensing client notification job");
            }
            try {
                threadPool.executor(executorName()).execute(new LicensingClientNotificationJob(true));
            } catch (EsRejectedExecutionException ex) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Couldn't re-schedule licensing client notification job", ex);
                }
            }
        }
    }

    //TODO: Shouldn't expose this
    public ESLicenseManager getEsLicenseManager() {
        return esLicenseManager;
    }

    public class LicensingClientNotificationJob implements Runnable {

        private final boolean reschedule;

        public LicensingClientNotificationJob(boolean reschedule) {
            this.reschedule = reschedule;
        }

        @Override
        public void run() {
            if (logger.isTraceEnabled()) {
                logger.trace("Performing LicensingClientNotificationJob");
            }

            if (clusterService.state().nodes().localNodeMaster()) {
                LicensesMetaData currentLicensesMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);

                // Change to debug
                logger.info("calling notifyFeatures from LicensingClientNotificationJob");
                long nextScheduleFrequency = Math.max(TimeValue.timeValueMinutes(5).getMillis(), notifyFeatures(currentLicensesMetaData));
                TimeValue updateFrequency = TimeValue.timeValueMillis(nextScheduleFrequency);

                if (this.reschedule) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Scheduling next run for licensing client notification job in: {}", updateFrequency.toString());
                    }
                    try {
                        threadPool.schedule(updateFrequency, executorName(), new SubmitReschedulingLicensingClientNotificationJob());
                    } catch (EsRejectedExecutionException ex) {
                        logger.debug("Reschedule licensing client notification job was rejected", ex);
                    }
                }
            }
        }
    }

    private long notifyFeatures(LicensesMetaData currentLicensesMetaData) {
        LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicensesMetaData);
        long nextScheduleFrequency = -1l;
        long offset = TimeValue.timeValueMinutes(1).getMillis();
        StringBuilder sb = new StringBuilder("Registered listeners: [ ");
        for (ListenerHolder listenerHolder : registeredListeners) {

            sb.append(listenerHolder.feature);
            sb.append(" ");

            long expiryDate = -1l;
            if (hasLicenseForFeature(listenerHolder.feature, currentLicensesMetaData)) {
                final Map<String, ESLicense> effectiveLicenses = getEffectiveLicenses(currentLicensesMetaData);
                expiryDate = effectiveLicenses.get(listenerHolder.feature).expiryDate();
            } else {
                final TrialLicense trialLicense = licensesWrapper.trialLicenses().getTrialLicense(listenerHolder.feature);
                if (trialLicense != null) {
                    expiryDate = trialLicense.expiryDate();
                }
            }
            long expiryDuration = expiryDate - System.currentTimeMillis();
            if (expiryDuration > 0l) {
                // Change to debug
                logger.info("calling enabledFeatureIfNeeded on " + listenerHolder.feature + " with trialLicense size=" + licensesWrapper.encodedTrialLicenses.size());
                listenerHolder.enableFeatureIfNeeded();
                if (nextScheduleFrequency == -1l) {
                    nextScheduleFrequency = expiryDuration + offset;
                } else {
                    nextScheduleFrequency = Math.min(expiryDuration + offset, nextScheduleFrequency);
                }
            } else {
                // Change to debug
                logger.info("calling disabledFeatureIfNeeded on " + listenerHolder.feature + " with trialLicense size=" + licensesWrapper.encodedTrialLicenses.size());
                listenerHolder.disableFeatureIfNeeded();
            }
        }
        sb.append("]");
        logger.info(sb.toString());

        if (nextScheduleFrequency == -1l) {
            nextScheduleFrequency = TimeValue.timeValueMinutes(5).getMillis();
        }

        return nextScheduleFrequency;
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
        final int durationInDays;
        final int maxNodes;

        public TrialLicenseOptions(int durationInDays, int maxNodes) {
            this.durationInDays = durationInDays;
            this.maxNodes = maxNodes;
        }
    }

    private static class ListenerHolder {
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

        public TrialLicenses trialLicenses() {
            return TrialLicenseUtils.fromEncodedTrialLicenses(encodedTrialLicenses);
        }

        /**
         * Check if any trial license for the feature exists,
         * if no trial license for feature exists, add new
         * trial license for feature
         *
         * @param trialLicense to add
         */
        public void addTrialLicense(TrialLicense trialLicense) {
            boolean featureTrialLicenseExists = false;
            for (TrialLicense currentTrialLicense : trialLicenses()) {
                if (currentTrialLicense.feature().equals(trialLicense.feature())) {
                    featureTrialLicenseExists = true;
                    break;
                }
            }
            if (!featureTrialLicenseExists) {
                this.encodedTrialLicenses = ImmutableSet.copyOf(Sets.union(encodedTrialLicenses,
                        Collections.singleton(TrialLicenseUtils.toEncodedTrialLicense(trialLicense))));
            }
        }

        public void addSignedLicenses(ESLicenseManager licenseManager, Set<ESLicense> newLicenses) {
            Set<ESLicense> currentSignedLicenses = signedLicenses(licenseManager);
            final ImmutableMap<String, ESLicense> licenseMap = reduceAndMap(Sets.union(currentSignedLicenses, newLicenses));
            this.signatures = licenseManager.toSignatures(licenseMap.values());
        }

        public void removeFeatures(ESLicenseManager licenseManager, Set<String> featuresToDelete) {
            Set<ESLicense> currentSignedLicenses = signedLicenses(licenseManager);
            final ImmutableMap<String, ESLicense> licenseMap = reduceAndMap(currentSignedLicenses);
            Set<ESLicense> licensesToDelete = new HashSet<>();
            for (Map.Entry<String, ESLicense> entry : licenseMap.entrySet()) {
                if (featuresToDelete.contains(entry.getKey())) {
                    licensesToDelete.add(entry.getValue());
                }
            }
            Set<ESLicense> reducedLicenses = Sets.difference(currentSignedLicenses, licensesToDelete);
            this.signatures = licenseManager.toSignatures(reducedLicenses);
        }

        public LicensesMetaData createLicensesMetaData() {
            return new LicensesMetaData(signatures, encodedTrialLicenses);
        }
    }


    private static class RegisterTrialLicenseRequest extends TransportRequest {
        private int maxNodes;
        private String feature;
        private TimeValue duration;

        private RegisterTrialLicenseRequest() {
        }

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

    //Should not be exposed; used by testing only
    public void clear() {
        if (notificationScheduler != null) {
            notificationScheduler.cancel(true);
            notificationScheduler = null;
        }
        registeredListeners.clear();

    }
}
