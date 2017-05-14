package org.elasticsearch.discovery;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.FaultDetection;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.DisruptedLinks;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TcpTransport;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public abstract class AbstractDisruptionTestCase extends ESIntegTestCase {

    static final TimeValue DISRUPTION_HEALING_OVERHEAD = TimeValue.timeValueSeconds(40); // we use 30s as timeout in many places.

    private ClusterDiscoveryConfiguration discoveryConfig;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(discoveryConfig.nodeSettings(nodeOrdinal))
                .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false).build();
    }

    @Before
    public void clearConfig() {
        discoveryConfig = null;
    }

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    private boolean disableBeforeIndexDeletion;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        disableBeforeIndexDeletion = false;
    }

    @Override
    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        if (scheme instanceof NetworkDisruption &&
                ((NetworkDisruption) scheme).getNetworkLinkDisruptionType() instanceof NetworkUnresponsive) {
            // the network unresponsive disruption may leave operations in flight
            // this is because this disruption scheme swallows requests by design
            // as such, these operations will never be marked as finished
            disableBeforeIndexDeletion = true;
        }
        super.setDisruptionScheme(scheme);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        if (disableBeforeIndexDeletion == false) {
            super.beforeIndexDeletion();
        }
    }

    List<String> startCluster(int numberOfNodes) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, -1);
    }

    List<String> startCluster(int numberOfNodes, int minimumMasterNode) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, minimumMasterNode, null);
    }

    List<String> startCluster(int numberOfNodes, int minimumMasterNode, @Nullable int[] unicastHostsOrdinals) throws
            ExecutionException, InterruptedException {
        configureCluster(numberOfNodes, unicastHostsOrdinals, minimumMasterNode);
        List<String> nodes = internalCluster().startNodes(numberOfNodes);
        ensureStableCluster(numberOfNodes);

        // TODO: this is a temporary solution so that nodes will not base their reaction to a partition based on previous successful results
        ZenPing zenPing = ((TestZenDiscovery) internalCluster().getInstance(Discovery.class)).getZenPing();
        if (zenPing instanceof UnicastZenPing) {
            ((UnicastZenPing) zenPing).clearTemporalResponses();
        }
        return nodes;
    }

    static final Settings DEFAULT_SETTINGS = Settings.builder()
            .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
            .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1") // for hitting simulated network failures quickly
            .put("discovery.zen.join_timeout", "10s")  // still long to induce failures but to long so test won't time out
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s") // <-- for hitting simulated network failures quickly
            .put(TcpTransport.TCP_CONNECT_TIMEOUT.getKey(), "10s") // Network delay disruption waits for the min between this
            // value and the time of disruption and does not recover immediately
            // when disruption is stop. We should make sure we recover faster
            // then the default of 30s, causing ensureGreen and friends to time out

            .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    void configureCluster(
            int numberOfNodes,
            @Nullable int[] unicastHostsOrdinals,
            int minimumMasterNode
    ) throws ExecutionException, InterruptedException {
        configureCluster(DEFAULT_SETTINGS, numberOfNodes, unicastHostsOrdinals, minimumMasterNode);
    }

    void configureCluster(
            Settings settings,
            int numberOfNodes,
            @Nullable int[] unicastHostsOrdinals,
            int minimumMasterNode
    ) throws ExecutionException, InterruptedException {
        if (minimumMasterNode < 0) {
            minimumMasterNode = numberOfNodes / 2 + 1;
        }
        logger.info("---> configured unicast");
        // TODO: Rarely use default settings form some of these
        Settings nodeSettings = Settings.builder()
                .put(settings)
                .put(NodeEnvironment.MAX_LOCAL_STORAGE_NODES_SETTING.getKey(), numberOfNodes)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minimumMasterNode)
                .build();

        if (discoveryConfig == null) {
            if (unicastHostsOrdinals == null) {
                discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(numberOfNodes, nodeSettings);
            } else {
                discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(numberOfNodes, nodeSettings, unicastHostsOrdinals);
            }
        }
    }

    /**
     * Simulates an unresponsive target node by dropping requests sent from source to target node.
     */
    public static class NetworkUnresponsive extends NetworkLinkDisruptionType {

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addUnresponsiveRule(targetTransportService);
        }

        @Override
        public String toString() {
            return "network unresponsive";
        }
    }

    /**
     * Simulates slow or congested network. Delivery of requests that are sent from source to target node are delayed by a configurable
     * time amount.
     */
    public static class NetworkDelay extends NetworkLinkDisruptionType {

        public static TimeValue DEFAULT_DELAY_MIN = TimeValue.timeValueSeconds(10);
        public static TimeValue DEFAULT_DELAY_MAX = TimeValue.timeValueSeconds(90);

        private final TimeValue delay;

        /**
         * Delays requests by a fixed time value.
         *
         * @param delay time to delay requests
         */
        public NetworkDelay(TimeValue delay) {
            this.delay = delay;
        }

        /**
         * Delays requests by a random but fixed time value between {@link #DEFAULT_DELAY_MIN} and {@link #DEFAULT_DELAY_MAX}.
         *
         * @param random instance to use for randomization of delay
         */
        public static NetworkDelay random(Random random) {
            return random(random, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX);
        }

        /**
         * Delays requests by a random but fixed time value between delayMin and delayMax.
         *
         * @param random   instance to use for randomization of delay
         * @param delayMin minimum delay
         * @param delayMax maximum delay
         */
        public static NetworkDelay random(Random random, TimeValue delayMin, TimeValue delayMax) {
            return new NetworkDelay(TimeValue.timeValueMillis(delayMin.millis() == delayMax.millis() ?
                    delayMin.millis() :
                    delayMin.millis() + random.nextInt((int) (delayMax.millis() - delayMin.millis()))));
        }

        @Override
        public void applyDisruption(MockTransportService sourceTransportService, MockTransportService targetTransportService) {
            sourceTransportService.addUnresponsiveRule(targetTransportService, delay);
        }

        @Override
        public TimeValue expectedTimeToHeal() {
            return delay;
        }

        @Override
        public String toString() {
            return "network delays for [" + delay + "]";
        }
    }

    ClusterState getNodeClusterState(String node) {
        return client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }

    void assertNoMaster(final String node) throws Exception {
        assertNoMaster(node, null, TimeValue.timeValueSeconds(10));
    }

    void assertNoMaster(final String node, TimeValue maxWaitTime) throws Exception {
        assertNoMaster(node, null, maxWaitTime);
    }

    void assertNoMaster(final String node, @Nullable final ClusterBlock expectedBlocks, TimeValue maxWaitTime) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = getNodeClusterState(node);
                final DiscoveryNodes nodes = state.nodes();
                assertNull("node [" + node + "] still has [" + nodes.getMasterNode() + "] as master", nodes.getMasterNode());
                if (expectedBlocks != null) {
                    for (ClusterBlockLevel level : expectedBlocks.levels()) {
                        assertTrue("node [" + node + "] does have level [" + level + "] in it's blocks", state.getBlocks().hasGlobalBlock
                                (level));
                    }
                }
            }
        }, maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    void assertDifferentMaster(final String node, final String oldMasterNode) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = getNodeClusterState(node);
                String masterNode = null;
                if (state.nodes().getMasterNode() != null) {
                    masterNode = state.nodes().getMasterNode().getName();
                }
                logger.trace("[{}] master is [{}]", node, state.nodes().getMasterNode());
                assertThat("node [" + node + "] still has [" + masterNode + "] as master",
                        oldMasterNode, not(equalTo(masterNode)));
            }
        }, 10, TimeUnit.SECONDS);
    }

    void assertMaster(String masterNode, List<String> nodes) throws Exception {
        assertBusy(() -> {
            for (String node : nodes) {
                ClusterState state = getNodeClusterState(node);
                String failMsgSuffix = "cluster_state:\n" + state;
                assertThat("wrong node count on [" + node + "]. " + failMsgSuffix, state.nodes().getSize(), equalTo(nodes.size()));
                String otherMasterNodeName = state.nodes().getMasterNode() != null ? state.nodes().getMasterNode().getName() : null;
                assertThat("wrong master on node [" + node + "]. " + failMsgSuffix, otherMasterNodeName, equalTo(masterNode));
            }
        });
    }

    public ServiceDisruptionScheme addRandomDisruptionScheme() {
        // TODO: add partial partitions
        NetworkDisruption p;
        final DisruptedLinks disruptedLinks;
        if (randomBoolean()) {
            disruptedLinks = TwoPartitions.random(random(), internalCluster().getNodeNames());
        } else {
            disruptedLinks = Bridge.random(random(), internalCluster().getNodeNames());
        }
        final NetworkLinkDisruptionType disruptionType;
        switch (randomInt(2)) {
            case 0:
                disruptionType = new NetworkUnresponsive();
                break;
            case 1:
                disruptionType = new NetworkDisconnect();
                break;
            case 2:
                disruptionType = NetworkDelay.random(random());
                break;
            default:
                throw new IllegalArgumentException();
        }
        final ServiceDisruptionScheme scheme;
        if (rarely()) {
            scheme = new SlowClusterStateProcessing(random());
        } else {
            scheme = new NetworkDisruption(disruptedLinks, disruptionType);
        }
        setDisruptionScheme(scheme);
        return scheme;
    }

    NetworkDisruption addRandomDisruptionType(TwoPartitions partitions) {
        final NetworkLinkDisruptionType disruptionType;
        if (randomBoolean()) {
            disruptionType = new NetworkUnresponsive();
        } else {
            disruptionType = new NetworkDisconnect();
        }
        NetworkDisruption partition = new NetworkDisruption(partitions, disruptionType);

        setDisruptionScheme(partition);

        return partition;
    }

    TwoPartitions isolateNode(String isolatedNode) {
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        side1.add(isolatedNode);
        side2.remove(isolatedNode);

        return new TwoPartitions(side1, side2);
    }

}
