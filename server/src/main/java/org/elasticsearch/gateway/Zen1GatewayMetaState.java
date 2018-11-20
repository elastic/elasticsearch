package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class Zen1GatewayMetaState extends GatewayMetaState implements ClusterStateApplier {

    private boolean writeWithoutComparingVersions;

    public Zen1GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader,
                                TransportService transportService) throws IOException {
        super(settings, nodeEnv, metaStateService, metaDataIndexUpgradeService, metaDataUpgrader, transportService);
        writeWithoutComparingVersions = true;
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (isMasterOrDataNode() == false) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            writeWithoutComparingVersions = true;
            return;
        }

        try {
            updateClusterState(event.state(), event.previousState(), writeWithoutComparingVersions);
            writeWithoutComparingVersions = false;
        } catch (WriteStateException e) {
            logger.warn("Exception occurred when storing new meta data", e);
        }
    }


}
