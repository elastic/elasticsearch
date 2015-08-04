/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.http.netty;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class HttpPublishPortIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Node.HTTP_ENABLED, true)
                .put("http.publish_port", 9080)
                .build();
    }

    @Test
    public void testHttpPublishPort() throws Exception {
        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().clear().setHttp(true).get();
        assertThat(response.getNodes(), arrayWithSize(greaterThanOrEqualTo(1)));
        NodeInfo nodeInfo = response.getNodes()[0];

        BoundTransportAddress address = nodeInfo.getHttp().address();
        assertThat(address.publishAddress(), instanceOf(InetSocketTransportAddress.class));

        InetSocketTransportAddress publishAddress = (InetSocketTransportAddress) address.publishAddress();
        assertThat(publishAddress.address().getPort(), is(9080));
    }
}
