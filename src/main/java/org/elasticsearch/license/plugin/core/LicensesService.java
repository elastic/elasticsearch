/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.core.trial.TrialLicenses;
import org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.license.core.ESLicenses.FeatureType;
import static org.elasticsearch.license.plugin.core.trial.TrialLicenses.TrialLicense;
import static org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder.trialLicensesBuilder;

/**
 * Service responsible for maintaining and providing access to licenses on nodes.
 *
 * TODO: Work in progress:
 *  - implement logic in clusterChanged
 *  - interface with LicenseManager
 */
@Singleton
public class LicensesService extends AbstractLifecycleComponent<LicensesService> implements ClusterStateListener, LicensesManagerService, LicensesClientService {

    private ESLicenseManager esLicenseManager;

    private ClusterService clusterService;

    private List<ListenerHolder> registeredListeners = new CopyOnWriteArrayList<>();

    @Inject
    public LicensesService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        this.esLicenseManager = ESLicenseManager.createClusterStateBasedInstance(clusterService);
    }

    /**
     * Registers new licenses in the cluster
     * <p/>
     * This method can be only called on the master node. It tries to create a new licenses on the master
     * and if it was successful it adds the license to cluster metadata.
     */
    @Override
    public void registerLicenses(final PutLicenseRequestHolder requestHolder, final ActionListener<ClusterStateUpdateResponse> listener) {
        final PutLicenseRequest request = requestHolder.request;
        final ESLicenses newLicenses = request.license();
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

                esLicenseManager.verifyLicenses(newLicenses);

                /*
                    Four cases:
                      - no metadata - just add new license
                      - only trial license - check if trial exists for feature; if so remove it to replace with signed
                      - only signed license - add signed license
                      - both trial & signed license - same as only trial & only signed case combined
                 */

                if (currentLicenses == null) {
                    // no licenses were registered
                    currentLicenses = new LicensesMetaData(newLicenses, null);
                } else {
                    TrialLicenses reducedTrialLicenses = null;
                    if (currentLicenses.getTrialLicenses() != null) {
                        // has trial licenses for some features; reduce trial licenses according to new signed licenses
                        reducedTrialLicenses = reduceTrialLicenses(newLicenses, currentLicenses.getTrialLicenses());
                    }
                    if (currentLicenses.getLicenses() != null) {
                        // merge previous signed license with new one
                        ESLicenses mergedLicenses = LicenseBuilders.merge(currentLicenses.getLicenses(), newLicenses);
                        currentLicenses = new LicensesMetaData(mergedLicenses, reducedTrialLicenses);
                    } else {
                        // no previous signed licenses to merge with
                        currentLicenses = new LicensesMetaData(newLicenses, reducedTrialLicenses);
                    }
                }

                mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicenses);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            /**
             * If signed license is found for any feature, remove the trial license for it
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

                if (currentLicenses != null) {
                    final ESLicenses newLicenses = LicenseBuilders.removeFeatures(currentLicenses.getLicenses(), featuresToDelete);
                    currentLicenses = new LicensesMetaData(newLicenses, currentLicenses.getTrialLicenses());
                }
                mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicenses);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }
        });
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
                if (trialLicenseCheck(trialLicense.feature().string())) {
                    TrialLicensesBuilder trialLicensesBuilder = TrialLicensesBuilder.trialLicensesBuilder().license(trialLicense);
                    if (currentLicenses != null) {
                        if (currentLicenses.getTrialLicenses() != null) {
                            // had previous trial licenses
                            trialLicensesBuilder = trialLicensesBuilder.licenses(currentLicenses.getTrialLicenses());
                            currentLicenses = new LicensesMetaData(currentLicenses.getLicenses(), trialLicensesBuilder.build());
                        } else {
                            // had no previous trial license
                            currentLicenses = new LicensesMetaData(currentLicenses.getLicenses(), trialLicensesBuilder.build());
                        }
                    } else {
                        // had no license meta data
                        currentLicenses = new LicensesMetaData(null, trialLicensesBuilder.build());
                    }
                }
                mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicenses);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {

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
        if ( DiscoveryNode.dataNode(settings) || DiscoveryNode.masterNode(settings)) {
            clusterService.add(this);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
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
    public void register(String feature, Listener listener) {
        registeredListeners.add(new ListenerHolder(feature, listener));
    }

    private void performMasterNodeOperations(ClusterChangedEvent event) {
        if (DiscoveryNode.masterNode(settings)) {
            // register all interested plugins
            for (ListenerHolder listenerHolder : registeredListeners) {
                if (listenerHolder.registered.compareAndSet(false, true)) {
                    if (!esLicenseManager.hasLicenseForFeature(FeatureType.fromString(listenerHolder.feature))) {
                        // does not have actual license so generate a trial license
                        TrialLicenseOptions options = listenerHolder.listener.trialLicenseOptions();
                        TrialLicense trialLicense = generateTrialLicense(listenerHolder.feature, options.durationInDays, options.maxNodes);
                        registerTrialLicense(trialLicense);
                    }
                }
            }

            // notify all interested plugins
            final LicensesMetaData currentLicensesMetaData = event.state().getMetaData().custom(LicensesMetaData.TYPE);
            if (currentLicensesMetaData != null) {
                notifyFeatures(currentLicensesMetaData);
            }
        }
    }

    //TODO: have a timed task to invoke listener.onDisabled upon latest expiry for a feature
    // currently dependant on clusterChangeEvents
    private void notifyFeatures(LicensesMetaData currentLicensesMetaData) {
        for (ListenerHolder listenerHolder : registeredListeners) {
            long expiryDate = -1l;
            if (esLicenseManager.hasLicenseForFeature(FeatureType.fromString(listenerHolder.feature))) {
                expiryDate = esLicenseManager.getExpiryDateForLicense(FeatureType.fromString(listenerHolder.feature));
            } else if (currentLicensesMetaData.getTrialLicenses() != null) {
                final TrialLicense trialLicense = currentLicensesMetaData.getTrialLicenses().getTrialLicense(FeatureType.fromString(listenerHolder.feature));
                if (trialLicense != null) {
                    expiryDate = trialLicense.expiryDate();
                }
            }
            if (expiryDate > System.currentTimeMillis()) {
                listenerHolder.listener.onEnabled();
            } else {
                listenerHolder.listener.onDisabled();
            }
        }
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
        final Listener listener;
        final AtomicBoolean registered = new AtomicBoolean(false);
        final AtomicBoolean trialLicenseGenerated = new AtomicBoolean(false);

        private ListenerHolder(String feature, Listener listener) {
            this.feature = feature;
            this.listener = listener;
        }
    }
}
