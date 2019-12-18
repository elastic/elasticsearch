package org.elasticsearch.client.cluster;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.SimpleConnectionStrategy;
import org.elasticsearch.transport.SniffConnectionStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.equalTo;

public class RemoteInfoResponseTests extends AbstractResponseTestCase<org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse,
        RemoteInfoResponse> {

    @Override
    protected org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse createServerTestInstance(XContentType xContentType) {
        int numRemoteInfos = randomIntBetween(0, 8);
        List<RemoteConnectionInfo> remoteInfos = new ArrayList<>();
        for (int i = 0; i < numRemoteInfos; i++) {
            remoteInfos.add(createRandomRemoteConnectionInfo());
        }
        return new org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse(remoteInfos);
    }

    @Override
    protected RemoteInfoResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return RemoteInfoResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.cluster.remote.RemoteInfoResponse serverTestInstance,
                                   RemoteInfoResponse clientInstance) {
        assertThat(clientInstance.getInfos().size(), equalTo(serverTestInstance.getInfos().size()));
        Map<String, RemoteConnectionInfo> serverInfos = serverTestInstance.getInfos().stream()
                .collect(toMap(RemoteConnectionInfo::getClusterAlias, identity()));
        for (RemoteConnectionInfo clientRemoteInfo : clientInstance.getInfos()) {
            RemoteConnectionInfo serverRemoteInfo = serverInfos.get(clientRemoteInfo.getClusterAlias());
            assertThat(clientRemoteInfo.getClusterAlias(), equalTo(serverRemoteInfo.getClusterAlias()));
            assertThat(clientRemoteInfo.getInitialConnectionTimeout(), equalTo(serverRemoteInfo.getInitialConnectionTimeout()));
            assertThat(clientRemoteInfo.isConnected(), equalTo(serverRemoteInfo.isConnected()));
            assertThat(clientRemoteInfo.isSkipUnavailable(), equalTo(serverRemoteInfo.isSkipUnavailable()));
            assertThat(clientRemoteInfo.getModeInfo().isConnected(), equalTo(serverRemoteInfo.getModeInfo().isConnected()));
            assertThat(clientRemoteInfo.getModeInfo().modeName(), equalTo(serverRemoteInfo.getModeInfo().modeName()));
            assertThat(clientRemoteInfo.getModeInfo().modeType(), equalTo(serverRemoteInfo.getModeInfo().modeType()));
            if (clientRemoteInfo.getModeInfo().modeType() == RemoteConnectionStrategy.ConnectionStrategy.SNIFF) {
                SniffConnectionStrategy.SniffModeInfo clientModeInfo =
                        (SniffConnectionStrategy.SniffModeInfo) clientRemoteInfo.getModeInfo();
                SniffConnectionStrategy.SniffModeInfo serverModeInfo =
                        (SniffConnectionStrategy.SniffModeInfo) serverRemoteInfo.getModeInfo();
                assertThat(clientModeInfo.getMaxConnectionsPerCluster(), equalTo(serverModeInfo.getMaxConnectionsPerCluster()));
                assertThat(clientModeInfo.getNumNodesConnected(), equalTo(serverModeInfo.getNumNodesConnected()));
                assertThat(clientModeInfo.getSeedNodes(), equalTo(serverModeInfo.getSeedNodes()));
            } else if (clientRemoteInfo.getModeInfo().modeType() == RemoteConnectionStrategy.ConnectionStrategy.SIMPLE) {
                SimpleConnectionStrategy.SimpleModeInfo clientModeInfo =
                        (SimpleConnectionStrategy.SimpleModeInfo) clientRemoteInfo.getModeInfo();
                SimpleConnectionStrategy.SimpleModeInfo serverModeInfo =
                        (SimpleConnectionStrategy.SimpleModeInfo) serverRemoteInfo.getModeInfo();
                assertThat(clientModeInfo.getAddresses(), equalTo(serverModeInfo.getAddresses()));
                assertThat(clientModeInfo.getMaxSocketConnections(), equalTo(serverModeInfo.getMaxSocketConnections()));
                assertThat(clientModeInfo.getNumSocketsConnected(), equalTo(serverModeInfo.getNumSocketsConnected()));
            } else {
                fail("impossible case");
            }
        }
    }

    private static RemoteConnectionInfo createRandomRemoteConnectionInfo() {
        RemoteConnectionInfo.ModeInfo modeInfo;
        if (randomBoolean()) {
            List<String> addresses = randomList(randomInt(8), () -> randomAlphaOfLength(8));
            int maxSocketConnections = randomInt(5);
            int numSocketsConnected = randomInt(5);
            modeInfo = new SimpleConnectionStrategy.SimpleModeInfo(addresses, maxSocketConnections, numSocketsConnected);
        } else {
            List<String> seedNodes = randomList(randomInt(8), () -> randomAlphaOfLength(8));
            int maxConnectionsPerCluster = randomInt(5);
            int numNodesConnected = randomInt(5);
            modeInfo = new SniffConnectionStrategy.SniffModeInfo(seedNodes, maxConnectionsPerCluster, numNodesConnected);
        }
        String clusterAlias = randomAlphaOfLength(8);
        TimeValue initialConnectionTimeout = TimeValue.parseTimeValue(randomTimeValue(), "randomInitialConnectionTimeout");
        boolean skipUnavailable = randomBoolean();
        return new RemoteConnectionInfo(clusterAlias, modeInfo, initialConnectionTimeout, skipUnavailable);
    }
}
