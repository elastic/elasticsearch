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

package org.elasticsearch.rest.action.admin.indices.upgrade;

import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.merge.policy.AbstractMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyModule;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class UpgradeTest extends ElasticsearchBackwardsCompatIntegrationTest {

    public void testUpgrade() throws Exception {
        int numIndexes = randomIntBetween(5, 10);
        for (int i = 0; i < numIndexes; ++i) {
            assertAcked(prepareCreate("test" + i).setSettings(ImmutableSettings.builder()
                // don't allow any merges so that we can check segments are upgraded
                // by the upgrader, and not just regular merging
                .put(MergePolicyModule.MERGE_POLICY_TYPE_KEY, NoMergePolicyProvider.class)));
            int numDocs = scaledRandomIntBetween(100, 1000);
            List<IndexRequestBuilder> builder = new ArrayList<>();
            for (int j = 0; j < numDocs; ++j) {
                String id = Integer.toString(j);
                builder.add(client().prepareIndex("test", "type1", id).setSource("text", "sometext"));
            }
            indexRandom(true, builder);
        }
        backwardsCluster().upgradeAllNodes();
        ensureGreen();
        
        HttpResponse rsp = httpClient().method("GET").path("/_upgrade").execute();
        System.out.println(rsp.toString());
        fail();

        // TODO:
        // - use GET /_upgrade to find which indexes need upgrading
        // - randomly select an index to upgrade, using wait_for_completion=false
        // - wait on upgrade with GET api
        // - confirm other indexes are still not upgraded
        // - check upgrade status with segments api
        // - upgrade the rest of the indexes, using wait_for_completion=true
        // - check upgrade status with segments api
        // - confirm GET shows all indexes upgraded

    }

    HttpRequestBuilder httpClient() {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);
        InetSocketAddress address = ((InetSocketTransportAddress) httpServerTransport.boundAddress().publishAddress()).address();
        return new HttpRequestBuilder(HttpClients.createDefault()).host(address.getHostName()).port(address.getPort());
    }
    

    public static class NoMergePolicyProvider extends AbstractMergePolicyProvider<NoMergePolicy> {
        @Inject
        public NoMergePolicyProvider(Store store, IndexSettingsService indexSettingsService) {
            super(store);
        }

        @Override
        public NoMergePolicy getMergePolicy() {
            return (NoMergePolicy) NoMergePolicy.INSTANCE;
        }

        @Override
        public void close() throws ElasticsearchException {
        }
    }
}
