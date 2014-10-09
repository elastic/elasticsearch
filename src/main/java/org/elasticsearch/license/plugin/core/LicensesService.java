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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.delete.DeleteLicenseRequest;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;

import java.util.HashSet;
import java.util.Set;

/**
 * Service responsible for maintaining and providing access to licenses on nodes.
 *
 * TODO: Work in progress:
 *  - implement logic in clusterChanged
 *  - interface with LicenseManager
 */
public class LicensesService extends AbstractLifecycleComponent<LicensesService> implements ClusterStateListener {

    private ESLicenseManager esLicenseManager;

    private InternalNode node;

    private ClusterService clusterService;

    @Inject
    public LicensesService(Settings settings, Node node) {
        super(settings);
        this.node = (InternalNode) node;
    }

    /**
     * Registers new licenses in the cluster
     * <p/>
     * This method can be only called on the master node. It tries to create a new licenses on the master
     * and if it was successful it adds the license to cluster metadata.
     */
    public void registerLicenses(String source, final PutLicenseRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final LicensesMetaData newLicenseMetaData = new LicensesMetaData(request.license());
        clusterService.submitStateUpdateTask(source, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
            @Override
            protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                return new ClusterStateUpdateResponse(acknowledged);
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData metaData = currentState.metaData();
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                LicensesMetaData currentLicenses = metaData.custom(LicensesMetaData.TYPE);

                esLicenseManager.verifyLicenses(newLicenseMetaData);

                if (currentLicenses == null) {
                    // no licenses were registered
                    currentLicenses = newLicenseMetaData;
                } else {
                    // merge previous license with new one
                    currentLicenses = new LicensesMetaData(LicenseBuilders.merge(currentLicenses, newLicenseMetaData));
                }

                mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicenses);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }
        });

    }

    public void unregisteredLicenses(String source, final DeleteLicenseRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        final Set<ESLicenses.FeatureType> featuresToDelete = asFeatureTypes(request.features());
        clusterService.submitStateUpdateTask(source, new AckedClusterStateUpdateTask<ClusterStateUpdateResponse>(request, listener) {
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
                    currentLicenses = new LicensesMetaData(LicenseBuilders.removeFeatures(currentLicenses, featuresToDelete));
                }
                mdBuilder.putCustom(LicensesMetaData.TYPE, currentLicenses);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }
        });
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        clusterService = node.injector().getInstance(ClusterService.class);
        esLicenseManager = ESLicenseManager.createClusterStateBasedInstance(clusterService);

        if (DiscoveryNode.dataNode(settings) || DiscoveryNode.masterNode(settings)) {
            clusterService.add(this);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        //TODO
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        //TODO
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        //TODO
    }

    private static Set<ESLicenses.FeatureType> asFeatureTypes(Set<String> featureTypeStrings) {
        Set<ESLicenses.FeatureType> featureTypes = new HashSet<>(featureTypeStrings.size());
        for (String featureString : featureTypeStrings) {
            featureTypes.add(ESLicenses.FeatureType.fromString(featureString));
        }
        return featureTypes;
    }
}
