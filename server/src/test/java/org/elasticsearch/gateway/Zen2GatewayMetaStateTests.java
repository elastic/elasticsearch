package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class Zen2GatewayMetaStateTests extends ESTestCase {
    private class Gateway extends Zen2GatewayMetaState {
        public Gateway(Settings settings, NodeEnvironment nodeEnvironment, TransportService transportService) throws IOException {
            super(settings, nodeEnvironment, new MetaStateService(nodeEnvironment, xContentRegistry()),
                    Mockito.mock(MetaDataIndexUpgradeService.class), Mockito.mock(MetaDataUpgrader.class),
                    transportService);
        }

        @Override
        protected void upgradeMetaData(MetaDataIndexUpgradeService metaDataIndexUpgradeService, MetaDataUpgrader metaDataUpgrader) {
            // do nothing
        }
    }

    private NodeEnvironment nodeEnvironment;
    private ClusterName clusterName;
    private Settings settings;
    private TransportService transportService;
    private DiscoveryNode localNode;

    @Override
    public void setUp() throws Exception {
        nodeEnvironment = newNodeEnvironment();
        localNode = new DiscoveryNode("node1", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNode.Role.MASTER), Version.CURRENT);
        clusterName = new ClusterName(randomAlphaOfLength(10));
        settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).build();
        transportService = Mockito.mock(TransportService.class);
        Mockito.when(transportService.getLocalNode()).thenReturn(localNode);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        nodeEnvironment.close();
        super.tearDown();
    }

    private Gateway newGateway() throws IOException {
        return new Gateway(settings, nodeEnvironment, transportService);
    }

    private Gateway maybeNew(Gateway gateway) throws IOException {
        if (randomBoolean()) {
            return newGateway();
        }
        return gateway;
    }

    public void testInitialState() throws IOException {
        Gateway gateway = newGateway();
        ClusterState state = gateway.getLastAcceptedState();
        assertThat(state.getClusterName(), equalTo(clusterName));
        assertTrue(MetaData.isGlobalStateEquals(state.metaData(), MetaData.EMPTY_META_DATA));
        assertThat(state.getVersion(), equalTo(Manifest.empty().getClusterStateVersion()));
        assertThat(state.getNodes().getLocalNode(), equalTo(localNode));

        long currentTerm = gateway.getCurrentTerm();
        assertThat(currentTerm, equalTo(Manifest.empty().getCurrentTerm()));
    }

    public void testSetCurrentTerm() throws IOException {
        Gateway gateway = newGateway();

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final long currentTerm = randomNonNegativeLong();
            gateway.setCurrentTerm(currentTerm);
            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
        }
    }

    private ClusterState createClusterState(long version, MetaData metaData) {
        return ClusterState.builder(clusterName).
                nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build()).
                version(version).
                metaData(metaData).
                build();
    }

    public void testSetLastAcceptedState() throws IOException {
        Gateway gateway = newGateway();

        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            final long version = randomNonNegativeLong();
            final MetaData metaData = MetaData.builder().
                    persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build()).
                    build();
            ClusterState state = createClusterState(version, metaData);

            gateway.setLastAcceptedState(state);
            gateway = maybeNew(gateway);
            assertThat(gateway.getLastAcceptedState().version(), equalTo(version));
            assertTrue(MetaData.isGlobalStateEquals(gateway.getLastAcceptedState().metaData(), metaData));
        }
    }

    public void testCurrentTermAndTermAreDifferent() {
        //TODO implement once CoordinationState is merged
    }

    public void testMarkAcceptedConfigAsCommitted() {
        //TODO implement once CoordinationState is merged
    }
}
