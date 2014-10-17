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
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.Utils;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.core.trial.TrialLicenseUtils;
import org.elasticsearch.license.plugin.core.trial.TrialLicenses;
import org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.license.core.ESLicenses.FeatureType;
import static org.elasticsearch.license.plugin.core.trial.TrialLicenses.TrialLicense;
import static org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder.trialLicensesBuilder;

/**
 * Service responsible for managing {@link org.elasticsearch.license.plugin.core.LicensesMetaData}
 * Interfaces through which this is exposed are:
 *  - LicensesManagerService - responsible for adding/deleting signed licenses
 *  - LicensesClientService - allow interested plugins (features) to register to licensing notifications
 *
 */
@Singleton
public class LicensesService extends AbstractLifecycleComponent<LicensesService> implements ClusterStateListener, LicensesManagerService, LicensesClientService {

    private ESLicenseManager esLicenseManager;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private List<ListenerHolder> registeredListeners = new CopyOnWriteArrayList<>();

    private volatile ScheduledFuture notificationScheduler;

    @Inject
    public LicensesService(Settings settings, ClusterService clusterService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.esLicenseManager = ESLicenseManager.createClusterStateBasedInstance(clusterService);
        this.threadPool = threadPool;
    }

    /**
     * Registers new licenses in the cluster
     * <p/>
     * This method can be only called on the master node. It tries to create a new licenses on the master
     * and if it was successful it adds the license to cluster metadata.
     */
    @Override
    public LicensesStatus registerLicenses(final PutLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener) {
        final PutLicenseRequest request = requestHolder.request;
        final ESLicenses newLicenses = request.license();
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
                licensesWrapper.addSignedLicenses(newLicenses);
                mdBuilder.putCustom(LicensesMetaData.TYPE, licensesWrapper.createLicensesMetaData());
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            /**
             * If signed license is found for any feature, remove the trial license for it
             * NOTE: not used
             * TODO: figure out desired behaviour for deleting trial licenses
             */
            private TrialLicenses reduceTrialLicenses(ESLicenses currentLicenses, TrialLicenses currentTrialLicenses) {
                TrialLicensesBuilder builder = trialLicensesBuilder();
                for (TrialLicense currentTrialLicense : currentTrialLicenses) {
                    if (currentLicenses.get(currentTrialLicense.feature()) == null) {
                        builder.license(currentTrialLicense);
                    }
                }
                return builder.build();
            }
        });
        return LicensesStatus.VALID;
    }

    @Override
    public void unregisterLicenses(final DeleteLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener) {
        final DeleteLicenseRequest request = requestHolder.request;
        final Set<FeatureType> featuresToDelete = asFeatureTypes(request.features());
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
                licensesWrapper.removeFeatures(featuresToDelete);
                mdBuilder.putCustom(LicensesMetaData.TYPE, licensesWrapper.createLicensesMetaData());
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }
        });
    }

    @Override
    public LicensesStatus checkLicenses(ESLicenses licenses) {
        LicensesStatus status = LicensesStatus.VALID;
        try {
            esLicenseManager.verifyLicenses(licenses);
        } catch (ExpiredLicenseException e) {
            status = LicensesStatus.EXPIRED;
        } catch (InvalidLicenseException e) {
            status = LicensesStatus.INVALID;
        }
        return status;
    }


    private void registerTrialLicense(final TrialLicense trialLicense) {
        clusterService.submitStateUpdateTask("register trial license []", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);
                final LicensesWrapper licensesWrapper = LicensesWrapper.wrap(currentLicenses);
                if (trialLicenseCheck(trialLicense.feature().string())) {
                    licensesWrapper.addTrialLicense(trialLicense);
                }
                mdBuilder.putCustom(LicensesMetaData.TYPE, licensesWrapper.createLicensesMetaData());
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                //TODO
                logger.info("LICENSING" + source, t);
            }

            private boolean trialLicenseCheck(String feature) {
                // check if actual license exists
                if (esLicenseManager.hasLicenseForFeature(FeatureType.fromString(feature))) {
                    return false;
                }
                // check if trial license for feature exists
                for (ListenerHolder holder : registeredListeners) {
                    if (holder.feature.equals(feature) && holder.registered.get()) {
                        if (holder.trialLicenseGenerated.compareAndSet(false, true)) {
                            return true;
                        }
                    }
                }
                return false;
            }
        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
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
        if (notificationScheduler != null) {
            notificationScheduler.cancel(true);
            notificationScheduler = null;
        }
        clusterService.remove(this);
        registeredListeners.clear();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            performMasterNodeOperations(event);
        }
    }

    private boolean checkIfUpdatedMetaData(ClusterChangedEvent event) {
        LicensesMetaData oldMetaData = event.previousState().getMetaData().custom(LicensesMetaData.TYPE);
        LicensesMetaData newMetaData = event.state().getMetaData().custom(LicensesMetaData.TYPE);
        return !((oldMetaData == null && newMetaData == null) || (oldMetaData != null && oldMetaData.equals(newMetaData)));
    }


    @Override
    public void register(String feature, TrialLicenseOptions trialLicenseOptions, Listener listener) {
        registeredListeners.add(new ListenerHolder(feature, trialLicenseOptions, listener));

        // DO we need to check STATE_NOT_RECOVERED_BLOCK here
        if (clusterService.state().nodes().localNodeMaster()) {
            LicensesMetaData currentMetaData = clusterService.state().metaData().custom(LicensesMetaData.TYPE);
            registerListeners(currentMetaData);
        }
    }

    private void registerListeners(LicensesMetaData currentMetaData) {
        for (ListenerHolder listenerHolder : registeredListeners) {
            if (listenerHolder.registered.compareAndSet(false, true)) {
                if (!esLicenseManager.hasLicenseForFeature(FeatureType.fromString(listenerHolder.feature))) {
                    // does not have actual license so generate a trial license
                    TrialLicenseOptions options = listenerHolder.trialLicenseOptions;
                    if (options != null) {
                        // Trial license option is provided
                        TrialLicense trialLicense = generateTrialLicense(listenerHolder.feature, options.durationInDays, options.maxNodes);
                        registerTrialLicense(trialLicense);
                    } else {
                        // notify feature as clusterChangedEvent may not happen
                        notifyFeatures(currentMetaData);
                    }
                }
            }
        }
    }

    private void performMasterNodeOperations(ClusterChangedEvent event) {
        if (event.state().nodes().localNodeMaster()) {
            final LicensesMetaData currentLicensesMetaData = event.state().getMetaData().custom(LicensesMetaData.TYPE);

            // register all interested plugins
            registerListeners(currentLicensesMetaData);

            // notify all interested plugins
            if (currentLicensesMetaData != null && checkIfUpdatedMetaData(event)) {
                long nextScheduleFrequency = notifyFeatures(currentLicensesMetaData);
                if (notificationScheduler == null) {
                    notificationScheduler = threadPool.schedule(TimeValue.timeValueMillis(nextScheduleFrequency), executorName(),
                            new SubmitReschedulingLicensingClientNotificationJob());
                }
            }
        }
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
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

                long nextScheduleFrequency = TimeValue.timeValueMinutes(5).getMillis();
                if (currentLicensesMetaData != null) {
                    nextScheduleFrequency = Math.max(nextScheduleFrequency, notifyFeatures(currentLicensesMetaData));
                }

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
        for (ListenerHolder listenerHolder : registeredListeners) {
            long expiryDate = -1l;
            if (esLicenseManager.hasLicenseForFeature(FeatureType.fromString(listenerHolder.feature))) {
                expiryDate = esLicenseManager.getExpiryDateForLicense(FeatureType.fromString(listenerHolder.feature));
            } else {
                final TrialLicense trialLicense = licensesWrapper.trialLicenses().getTrialLicense(FeatureType.fromString(listenerHolder.feature));
                if (trialLicense != null) {
                    expiryDate = trialLicense.expiryDate();
                }
            }
            long expiryDuration = expiryDate - System.currentTimeMillis();
            if (expiryDuration > 0l) {
                listenerHolder.enableFeatureIfNeeded();
                if (nextScheduleFrequency == -1l) {
                    nextScheduleFrequency = expiryDuration + offset;
                } else {
                    nextScheduleFrequency = Math.min(expiryDuration + offset, nextScheduleFrequency);
                }
            } else {
                listenerHolder.disableFeatureIfNeeded();
            }
        }

        if (nextScheduleFrequency == -1l) {
            nextScheduleFrequency = TimeValue.timeValueMinutes(5).getMillis();
        }

        return nextScheduleFrequency;
    }

    private static Set<FeatureType> asFeatureTypes(Set<String> featureTypeStrings) {
        Set<FeatureType> featureTypes = new HashSet<>(featureTypeStrings.size());
        for (String featureString : featureTypeStrings) {
            featureTypes.add(FeatureType.fromString(featureString));
        }
        return featureTypes;
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

    private TrialLicense generateTrialLicense(String feature, int durationInDays, int maxNodes) {
        return TrialLicensesBuilder.trialLicenseBuilder()
                .issuedTo(clusterService.state().getClusterName().value())
                .issueDate(System.currentTimeMillis())
                .durationInDays(durationInDays)
                .feature(FeatureType.fromString(feature))
                .maxNodes(maxNodes)
                .build();
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
        final AtomicBoolean registered = new AtomicBoolean(false);
        final AtomicBoolean trialLicenseGenerated = new AtomicBoolean(false);

        final AtomicBoolean toggle = new AtomicBoolean(false);
        final AtomicBoolean initialState = new AtomicBoolean(true);

        private ListenerHolder(String feature, TrialLicenseOptions trialLicenseOptions, Listener listener) {
            this.feature = feature;
            this.trialLicenseOptions = trialLicenseOptions;
            this.listener = listener;
        }

        private void enableFeatureIfNeeded() {
            if (toggle.compareAndSet(false, true) || initialState.compareAndSet(true, false)) {
                listener.onEnabled();
                // needed as toggle may not be set
                toggle.set(true);
            }
        }

        private void disableFeatureIfNeeded() {
            if (toggle.compareAndSet(true, false) || initialState.compareAndSet(true, false)) {
                listener.onDisabled();
                // needed as toggle may not be set
                toggle.set(false);
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
                this.signatures = ImmutableSet.copyOf(licensesMetaData.signatures);
                this.encodedTrialLicenses = ImmutableSet.copyOf(licensesMetaData.encodedTrialLicenses);
            }
        }

        public ESLicenses signedLicenses() {
            return org.elasticsearch.license.manager.Utils.fromSignatures(signatures);
        }

        public TrialLicenses trialLicenses() {
            return TrialLicenseUtils.fromEncodedTrialLicenses(encodedTrialLicenses);
        }

        public void addTrialLicense(TrialLicense trialLicense) {
            this.encodedTrialLicenses = ImmutableSet.copyOf(Sets.union(encodedTrialLicenses,
                    Collections.singleton(TrialLicenseUtils.toEncodedTrialLicense(trialLicense))));
        }

        public void addSignedLicenses(ESLicenses licenses) {
            ESLicenses currentSignedLicenses = signedLicenses();
            final ESLicenses mergedLicenses = LicenseBuilders.merge(currentSignedLicenses, licenses);
            Set<String> newSignatures = Sets.newHashSet(Utils.toSignatures(mergedLicenses));
            this.signatures = ImmutableSet.copyOf(Sets.union(signatures, newSignatures));
        }

        public void removeFeatures(Set<FeatureType> featuresToDelete) {
            ESLicenses currentSignedLicenses = signedLicenses();
            final ESLicenses reducedLicenses = LicenseBuilders.removeFeatures(currentSignedLicenses, featuresToDelete);
            Set<String> reducedSignatures = Sets.newHashSet(Utils.toSignatures(reducedLicenses));
            this.signatures = ImmutableSet.copyOf(Sets.intersection(signatures, reducedSignatures));
        }

        public LicensesMetaData createLicensesMetaData() {
            return new LicensesMetaData(signatures, encodedTrialLicenses);
        }
    }

    public void clear() {
        if (notificationScheduler != null) {
            notificationScheduler.cancel(true);
            notificationScheduler = null;
        }
        registeredListeners.clear();

    }
}
