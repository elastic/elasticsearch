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

package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.core.IngestInfo;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class MultiNodePutPipelineIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // TODO: Remove this method once gets in: https://github.com/elastic/elasticsearch/issues/16019
        if (nodeOrdinal % 2 == 0) {
            return Settings.builder().put("", "").put(super.nodeSettings(nodeOrdinal)).build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(IngestGeoIpPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Collections.singletonList(TestSeedPlugin.class);
    }

    public void test() throws Exception {
        NodesInfoRequest req = new NodesInfoRequest().clear().ingest(true);
        NodesInfoResponse response = client().admin().cluster().nodesInfo(req).actionGet();
        IngestInfo ingestInfo = response.getNodes()[0].getIngest();
        assertThat(ingestInfo.getProcessors(), equalTo("hello"));
    }


}
