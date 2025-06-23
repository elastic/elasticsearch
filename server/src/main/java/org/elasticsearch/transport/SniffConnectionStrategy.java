/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;

public class SniffConnectionStrategy extends RemoteConnectionStrategy {

    /**
     * A list of initial seed nodes to discover eligible nodes from the remote cluster
     */
    public static final Setting.AffixSetting<List<String>> REMOTE_CLUSTER_SEEDS = Setting.affixKeySetting(
        "cluster.remote.",
        "seeds",
        (ns, key) -> Setting.listSetting(key, Collections.emptyList(), s -> {
            // validate seed address
            parsePort(s);
            return s;
        }, new StrategyValidator<>(ns, key, ConnectionStrategy.SNIFF), Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    /**
     * A proxy address for the remote cluster. By default this is not set, meaning that Elasticsearch will connect directly to the nodes in
     * the remote cluster using their publish addresses. If this setting is set to an IP address or hostname then Elasticsearch will connect
     * to the nodes in the remote cluster using this address instead. Use of this setting is not recommended and it is deliberately
     * undocumented as it does not work well with all proxies.
     */
    public static final Setting.AffixSetting<String> REMOTE_CLUSTERS_PROXY = Setting.affixKeySetting(
        "cluster.remote.",
        "proxy",
        (ns, key) -> Setting.simpleString(key, new StrategyValidator<>(ns, key, ConnectionStrategy.SNIFF, s -> {
            if (Strings.hasLength(s)) {
                parsePort(s);
            }
        }), Setting.Property.Dynamic, Setting.Property.NodeScope),
        () -> REMOTE_CLUSTER_SEEDS
    );

    /**
     * The maximum number of connections that will be established to a remote cluster. For instance if there is only a single
     * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
     */
    public static final Setting<Integer> REMOTE_CONNECTIONS_PER_CLUSTER = intSetting(
        "cluster.remote.connections_per_cluster",
        3,
        1,
        Setting.Property.NodeScope
    );
    /**
     * The maximum number of node connections that will be established to a remote cluster. For instance if there is only a single
     * seed node, other nodes will be discovered up to the given number of nodes in this setting. The default is 3.
     */
    public static final Setting.AffixSetting<Integer> REMOTE_NODE_CONNECTIONS = Setting.affixKeySetting(
        "cluster.remote.",
        "node_connections",
        (ns, key) -> intSetting(
            key,
            REMOTE_CONNECTIONS_PER_CLUSTER,
            1,
            new StrategyValidator<>(ns, key, ConnectionStrategy.SNIFF),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        )
    );

    static final int CHANNELS_PER_CONNECTION = 6;

    private static final TimeValue SNIFF_REQUEST_TIMEOUT = TimeValue.THIRTY_SECONDS; // TODO make configurable?

    private static final Predicate<DiscoveryNode> DEFAULT_NODE_PREDICATE = (node) -> Version.CURRENT.isCompatible(node.getVersion())
        && (node.isMasterNode() == false || node.canContainData() || node.isIngestNode());

    private final List<String> configuredSeedNodes;
    private final List<Supplier<DiscoveryNode>> seedNodes;
    private final int maxNumRemoteConnections;
    private final Predicate<DiscoveryNode> nodePredicate;
    private final SetOnce<ClusterName> remoteClusterName = new SetOnce<>();
    private final String proxyAddress;
    private final Executor managementExecutor;

    SniffConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        Settings settings
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterAlias).get(settings),
            settings,
            REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(settings),
            getNodePredicate(settings),
            REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings)
        );
    }

    SniffConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        String proxyAddress,
        Settings settings,
        int maxNumRemoteConnections,
        Predicate<DiscoveryNode> nodePredicate,
        List<String> configuredSeedNodes
    ) {
        this(
            clusterAlias,
            transportService,
            connectionManager,
            proxyAddress,
            settings,
            maxNumRemoteConnections,
            nodePredicate,
            configuredSeedNodes,
            configuredSeedNodes.stream()
                .map(seedAddress -> (Supplier<DiscoveryNode>) () -> resolveSeedNode(clusterAlias, seedAddress, proxyAddress))
                .toList()
        );
    }

    SniffConnectionStrategy(
        String clusterAlias,
        TransportService transportService,
        RemoteConnectionManager connectionManager,
        String proxyAddress,
        Settings settings,
        int maxNumRemoteConnections,
        Predicate<DiscoveryNode> nodePredicate,
        List<String> configuredSeedNodes,
        List<Supplier<DiscoveryNode>> seedNodes
    ) {
        super(clusterAlias, transportService, connectionManager, settings);
        this.proxyAddress = proxyAddress;
        this.maxNumRemoteConnections = maxNumRemoteConnections;
        this.nodePredicate = nodePredicate;
        this.configuredSeedNodes = configuredSeedNodes;
        this.seedNodes = seedNodes;
        this.managementExecutor = transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT);
    }

    static Stream<Setting.AffixSetting<?>> enablementSettings() {
        return Stream.of(SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS);
    }

    static Writeable.Reader<RemoteConnectionInfo.ModeInfo> infoReader() {
        return SniffModeInfo::new;
    }

    @Override
    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumRemoteConnections;
    }

    @Override
    protected boolean strategyMustBeRebuilt(Settings newSettings) {
        String proxy = REMOTE_CLUSTERS_PROXY.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        List<String> addresses = REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        int nodeConnections = REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(clusterAlias).get(newSettings);
        return nodeConnections != maxNumRemoteConnections
            || seedsChanged(configuredSeedNodes, addresses)
            || proxyChanged(proxyAddress, proxy);
    }

    @Override
    protected ConnectionStrategy strategyType() {
        return ConnectionStrategy.SNIFF;
    }

    @Override
    protected void connectImpl(ActionListener<Void> listener) {
        collectRemoteNodes(seedNodes.iterator(), listener);
    }

    @Override
    protected RemoteConnectionInfo.ModeInfo getModeInfo() {
        return new SniffModeInfo(configuredSeedNodes, maxNumRemoteConnections, connectionManager.size());
    }

    private void collectRemoteNodes(Iterator<Supplier<DiscoveryNode>> seedNodesSuppliers, ActionListener<Void> listener) {
        if (Thread.currentThread().isInterrupted()) {
            listener.onFailure(new InterruptedException("remote connect thread got interrupted"));
            return;
        }

        if (seedNodesSuppliers.hasNext()) {
            final Consumer<Exception> onFailure = e -> {
                if (isRetryableException(e) && seedNodesSuppliers.hasNext()) {
                    logger.debug(() -> "fetching nodes from external cluster [" + clusterAlias + "] failed moving to next seed node", e);
                    collectRemoteNodes(seedNodesSuppliers, listener);
                } else {
                    logger.warn(() -> "fetching nodes from external cluster [" + clusterAlias + "] failed", e);
                    listener.onFailure(e);
                }
            };

            final DiscoveryNode seedNode = seedNodesSuppliers.next().get();
            logger.trace("[{}] opening transient connection to seed node: [{}]", clusterAlias, seedNode);
            final ListenableFuture<Transport.Connection> openConnectionStep = new ListenableFuture<>();
            try {
                connectionManager.openConnection(seedNode, null, openConnectionStep);
            } catch (Exception e) {
                onFailure.accept(e);
            }

            final ListenableFuture<TransportService.HandshakeResponse> handshakeStep = new ListenableFuture<>();
            openConnectionStep.addListener(ActionListener.wrap(connection -> {
                ConnectionProfile connectionProfile = connectionManager.getConnectionProfile();
                transportService.handshake(
                    connection,
                    connectionProfile.getHandshakeTimeout(),
                    getRemoteClusterNamePredicate(),
                    handshakeStep
                );
            }, onFailure));

            final ListenableFuture<Void> fullConnectionStep = new ListenableFuture<>();
            handshakeStep.addListener(ActionListener.wrap(handshakeResponse -> {
                final DiscoveryNode handshakeNode = handshakeResponse.getDiscoveryNode();

                if (nodePredicate.test(handshakeNode) && shouldOpenMoreConnections()) {
                    logger.trace(
                        "[{}] opening managed connection to seed node: [{}] proxy address: [{}]",
                        clusterAlias,
                        handshakeNode,
                        proxyAddress
                    );
                    final DiscoveryNode handshakeNodeWithProxy = maybeAddProxyAddress(proxyAddress, handshakeNode);
                    connectionManager.connectToRemoteClusterNode(
                        handshakeNodeWithProxy,
                        getConnectionValidator(handshakeNodeWithProxy),
                        fullConnectionStep
                    );
                } else {
                    fullConnectionStep.onResponse(null);
                }
            }, e -> {
                final Transport.Connection connection = openConnectionStep.result();
                final DiscoveryNode node = connection.getNode();
                logger.debug(() -> format("[%s] failed to handshake with seed node: [%s]", clusterAlias, node), e);
                IOUtils.closeWhileHandlingException(connection);
                onFailure.accept(e);
            }));

            fullConnectionStep.addListener(ActionListener.wrap(aVoid -> {
                if (remoteClusterName.get() == null) {
                    TransportService.HandshakeResponse handshakeResponse = handshakeStep.result();
                    assert handshakeResponse.getClusterName().value() != null;
                    remoteClusterName.set(handshakeResponse.getClusterName());
                }
                final Transport.Connection connection = openConnectionStep.result();

                // here we pass on the connection since we can only close it once the sendRequest returns otherwise
                // due to the async nature (it will return before it's actually sent) this can cause the request to fail
                // due to an already closed connection.
                ThreadPool threadPool = transportService.getThreadPool();
                ThreadContext threadContext = threadPool.getThreadContext();

                final String action;
                final TransportRequest request;
                final AbstractSniffResponseHandler<?> sniffResponseHandler;
                // Use different action to collect nodes information depending on the connection model
                if (REMOTE_CLUSTER_PROFILE.equals(connectionManager.getConnectionProfile().getTransportProfile())) {
                    action = RemoteClusterNodesAction.TYPE.name();
                    request = RemoteClusterNodesAction.Request.REMOTE_CLUSTER_SERVER_NODES;
                    sniffResponseHandler = new RemoteClusterNodesSniffResponseHandler(connection, listener, seedNodesSuppliers);
                } else {
                    action = ClusterStateAction.NAME;
                    final ClusterStateRequest clusterStateRequest = new ClusterStateRequest(SNIFF_REQUEST_TIMEOUT);
                    clusterStateRequest.clear();
                    clusterStateRequest.nodes(true);
                    request = clusterStateRequest;
                    sniffResponseHandler = new ClusterStateSniffResponseHandler(connection, listener, seedNodesSuppliers);
                }

                try (var ignored = threadContext.newEmptySystemContext()) {
                    // we stash any context here since this is an internal execution and should not leak any existing context information.
                    transportService.sendRequest(
                        connection,
                        action,
                        request,
                        TransportRequestOptions.EMPTY,
                        new TransportService.ContextRestoreResponseHandler<>(
                            threadContext.newRestorableContext(false),
                            sniffResponseHandler
                        )
                    );
                }
            }, e -> {
                final Transport.Connection connection = openConnectionStep.result();
                final DiscoveryNode node = connection.getNode();
                logger.debug(() -> format("[%s] failed to open managed connection to seed node: [%s]", clusterAlias, node), e);
                IOUtils.closeWhileHandlingException(connection);
                onFailure.accept(e);
            }));
        } else {
            listener.onFailure(new NoSeedNodeLeftException("no seed node left for cluster: [" + clusterAlias + "]"));
        }
    }

    private ConnectionManager.ConnectionValidator getConnectionValidator(DiscoveryNode node) {
        return (connection, profile, listener) -> {
            assert profile.getTransportProfile().equals(connectionManager.getConnectionProfile().getTransportProfile())
                : "transport profile must be consistent between the connection manager and the actual profile";
            transportService.connectionValidator(node)
                .validate(
                    RemoteConnectionManager.wrapConnectionWithRemoteClusterInfo(
                        connection,
                        clusterAlias,
                        connectionManager.getCredentialsManager()
                    ),
                    profile,
                    listener
                );
        };
    }

    private class RemoteClusterNodesSniffResponseHandler extends AbstractSniffResponseHandler<RemoteClusterNodesAction.Response> {
        RemoteClusterNodesSniffResponseHandler(
            Transport.Connection connection,
            ActionListener<Void> listener,
            Iterator<Supplier<DiscoveryNode>> seedNodes
        ) {
            super(connection, listener, seedNodes);
        }

        @Override
        public RemoteClusterNodesAction.Response read(StreamInput in) throws IOException {
            return new RemoteClusterNodesAction.Response(in);
        }

        @Override
        public void handleResponse(RemoteClusterNodesAction.Response response) {
            handleNodes(response.getNodes().iterator());
        }
    }

    private class ClusterStateSniffResponseHandler extends AbstractSniffResponseHandler<ClusterStateResponse> {
        ClusterStateSniffResponseHandler(
            Transport.Connection connection,
            ActionListener<Void> listener,
            Iterator<Supplier<DiscoveryNode>> seedNodes
        ) {
            super(connection, listener, seedNodes);
        }

        @Override
        public ClusterStateResponse read(StreamInput in) throws IOException {
            return new ClusterStateResponse(in);
        }

        @Override
        public void handleResponse(ClusterStateResponse response) {
            handleNodes(response.getState().nodes().getNodes().values().iterator());
        }
    }

    /* This class handles the nodes response from the remote cluster when sniffing nodes to connect to */
    private abstract class AbstractSniffResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final Transport.Connection connection;
        private final ActionListener<Void> listener;
        private final Iterator<Supplier<DiscoveryNode>> seedNodes;

        AbstractSniffResponseHandler(
            Transport.Connection connection,
            ActionListener<Void> listener,
            Iterator<Supplier<DiscoveryNode>> seedNodes
        ) {
            this.connection = connection;
            this.listener = listener;
            this.seedNodes = seedNodes;
        }

        protected void handleNodes(Iterator<DiscoveryNode> nodesIter) {
            while (nodesIter.hasNext()) {
                final DiscoveryNode node = nodesIter.next();
                if (nodePredicate.test(node) && shouldOpenMoreConnections()) {
                    logger.trace("[{}] opening managed connection to node: [{}] proxy address: [{}]", clusterAlias, node, proxyAddress);
                    final DiscoveryNode nodeWithProxy = maybeAddProxyAddress(proxyAddress, node);
                    connectionManager.connectToRemoteClusterNode(nodeWithProxy, getConnectionValidator(node), new ActionListener<>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            handleNodes(nodesIter);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof ConnectTransportException || e instanceof IllegalStateException) {
                                // ISE if we fail the handshake with an version incompatible node
                                // fair enough we can't connect just move on
                                logger.debug(() -> format("[%s] failed to open managed connection to node [%s]", clusterAlias, node), e);
                                handleNodes(nodesIter);
                            } else {
                                logger.warn(() -> format("[%s] failed to open managed connection to node [%s]", clusterAlias, node), e);
                                IOUtils.closeWhileHandlingException(connection);
                                collectRemoteNodes(seedNodes, listener);
                            }
                        }
                    });
                    return;
                }
            }
            // We have to close this connection before we notify listeners - this is mainly needed for test correctness
            // since if we do it afterwards we might fail assertions that check if all high level connections are closed.
            // from a code correctness perspective we could also close it afterwards.
            IOUtils.closeWhileHandlingException(connection);
            int openConnections = connectionManager.size();
            if (openConnections == 0) {
                listener.onFailure(new IllegalStateException("Unable to open any connections to remote cluster [" + clusterAlias + "]"));
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            logger.warn(() -> "fetching nodes from external cluster " + clusterAlias + " failed", exp);
            try {
                IOUtils.closeWhileHandlingException(connection);
            } finally {
                // once the connection is closed lets try the next node
                collectRemoteNodes(seedNodes, listener);
            }
        }

        @Override
        public Executor executor() {
            return managementExecutor;
        }
    }

    private Predicate<ClusterName> getRemoteClusterNamePredicate() {
        return new Predicate<>() {
            @Override
            public boolean test(ClusterName c) {
                return remoteClusterName.get() == null || c.equals(remoteClusterName.get());
            }

            @Override
            public String toString() {
                return remoteClusterName.get() == null
                    ? "any cluster name"
                    : "expected remote cluster name [" + remoteClusterName.get().value() + "]";
            }
        };
    }

    private static DiscoveryNode resolveSeedNode(String clusterAlias, String address, String proxyAddress) {
        var seedVersion = new VersionInformation(
            Version.CURRENT.minimumCompatibilityVersion(),
            IndexVersions.MINIMUM_COMPATIBLE,
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersion.current()
        );
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            TransportAddress transportAddress = new TransportAddress(parseConfiguredAddress(address));
            return new DiscoveryNode(
                null,
                clusterAlias + "#" + transportAddress,
                transportAddress,
                Collections.emptyMap(),
                DiscoveryNodeRole.roles(),
                seedVersion
            );
        } else {
            TransportAddress transportAddress = new TransportAddress(parseConfiguredAddress(proxyAddress));
            String hostName = RemoteConnectionStrategy.parseHost(proxyAddress);
            return new DiscoveryNode(
                null,
                clusterAlias + "#" + address,
                UUIDs.randomBase64UUID(),
                hostName,
                address,
                transportAddress,
                Collections.singletonMap("server_name", hostName),
                DiscoveryNodeRole.roles(),
                seedVersion
            );
        }
    }

    // Default visibility for tests
    static Predicate<DiscoveryNode> getNodePredicate(Settings settings) {
        if (RemoteClusterService.REMOTE_NODE_ATTRIBUTE.exists(settings)) {
            // nodes can be tagged with node.attr.remote_gateway: true to allow a node to be a gateway node for cross cluster search
            String attribute = RemoteClusterService.REMOTE_NODE_ATTRIBUTE.get(settings);
            return DEFAULT_NODE_PREDICATE.and((node) -> Booleans.parseBoolean(node.getAttributes().getOrDefault(attribute, "false")));
        }
        return DEFAULT_NODE_PREDICATE;
    }

    private static DiscoveryNode maybeAddProxyAddress(String proxyAddress, DiscoveryNode node) {
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            return node;
        } else {
            // resolve proxy address lazy here
            InetSocketAddress proxyInetAddress = parseConfiguredAddress(proxyAddress);
            return new DiscoveryNode(
                node.getName(),
                node.getId(),
                node.getEphemeralId(),
                node.getHostName(),
                node.getHostAddress(),
                new TransportAddress(proxyInetAddress),
                node.getAttributes(),
                node.getRoles(),
                node.getVersionInformation()
            );
        }
    }

    private static boolean seedsChanged(final List<String> oldSeedNodes, final List<String> newSeedNodes) {
        if (oldSeedNodes.size() != newSeedNodes.size()) {
            return true;
        }
        Set<String> oldSeeds = new HashSet<>(oldSeedNodes);
        Set<String> newSeeds = new HashSet<>(newSeedNodes);
        return oldSeeds.equals(newSeeds) == false;
    }

    private static boolean proxyChanged(String oldProxy, String newProxy) {
        if (oldProxy == null || oldProxy.isEmpty()) {
            return (newProxy == null || newProxy.isEmpty()) == false;
        }

        return Objects.equals(oldProxy, newProxy) == false;
    }

    public static class SniffModeInfo implements RemoteConnectionInfo.ModeInfo {

        final List<String> seedNodes;
        final int maxConnectionsPerCluster;
        final int numNodesConnected;

        public SniffModeInfo(List<String> seedNodes, int maxConnectionsPerCluster, int numNodesConnected) {
            this.seedNodes = seedNodes;
            this.maxConnectionsPerCluster = maxConnectionsPerCluster;
            this.numNodesConnected = numNodesConnected;
        }

        private SniffModeInfo(StreamInput input) throws IOException {
            seedNodes = Arrays.asList(input.readStringArray());
            maxConnectionsPerCluster = input.readVInt();
            numNodesConnected = input.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray("seeds");
            for (String address : seedNodes) {
                builder.value(address);
            }
            builder.endArray();
            builder.field("num_nodes_connected", numNodesConnected);
            builder.field("max_connections_per_cluster", maxConnectionsPerCluster);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(seedNodes);
            out.writeVInt(maxConnectionsPerCluster);
            out.writeVInt(numNodesConnected);
        }

        @Override
        public boolean isConnected() {
            return numNodesConnected > 0;
        }

        @Override
        public String modeName() {
            return "sniff";
        }

        @Override
        public RemoteConnectionStrategy.ConnectionStrategy modeType() {
            return RemoteConnectionStrategy.ConnectionStrategy.SNIFF;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SniffModeInfo sniff = (SniffModeInfo) o;
            return maxConnectionsPerCluster == sniff.maxConnectionsPerCluster
                && numNodesConnected == sniff.numNodesConnected
                && Objects.equals(seedNodes, sniff.seedNodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seedNodes, maxConnectionsPerCluster, numNodesConnected);
        }
    }
}
