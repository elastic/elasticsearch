package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;

public class Zen2GatewayMetaState extends GatewayMetaState implements CoordinationState.PersistedState {
    public Zen2GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader,
                                TransportService transportService) throws IOException {
        super(settings, nodeEnv, metaStateService, metaDataIndexUpgradeService, metaDataUpgrader, transportService);
    }

    @Override
    public long getCurrentTerm() {
        return previousManifest.getCurrentTerm();
    }

    @Override
    public ClusterState getLastAcceptedState() {
        return previousClusterState;
    }

    @Override
    public void setCurrentTerm(long currentTerm) {
        Manifest manifest = new Manifest(currentTerm, previousManifest.getClusterStateVersion(), previousManifest.getGlobalGeneration(),
                new HashMap<>(previousManifest.getIndexGenerations()));
        try {
            metaStateService.writeManifestAndCleanup("current term changed", manifest);
            previousManifest = manifest;
        } catch (WriteStateException e) {
            logger.warn("Exception occurred when setting current term", e);
            //TODO re-throw exception
        }
    }

    @Override
    public void setLastAcceptedState(ClusterState clusterState) {
        assert clusterState.blocks().disableStatePersistence() == false;

        try {
            boolean writeWithoutComparingVersions = previousClusterState.term() != clusterState.term();
            updateClusterState(clusterState, previousClusterState, writeWithoutComparingVersions);
            previousClusterState = clusterState;
        } catch (WriteStateException e) {
            logger.warn("Exception occurred when setting last accepted state", e);
            //TODO re-throw exception
        }
    }
}
