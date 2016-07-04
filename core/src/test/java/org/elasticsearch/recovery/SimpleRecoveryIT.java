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

package org.elasticsearch.recovery;

import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.client.Requests.flushRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class SimpleRecoveryIT extends ESIntegTestCase {
    @Override
    public Settings indexSettings() {
        return Settings.builder().put(super.indexSettings()).put(recoverySettings()).build();
    }

    protected Settings recoverySettings() {
        return Settings.Builder.EMPTY_SETTINGS;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    public void testSimpleRecovery() throws Exception {
        assertAcked(prepareCreate("test", 1).execute().actionGet());

        NumShards numShards = getNumShards("test");

        logger.info("Running Cluster Health");
        ensureYellow();

        client().index(indexRequest("test").type("type1").id("1").source(source("1", "test"))).actionGet();
        FlushResponse flushResponse = client().admin().indices().flush(flushRequest("test")).actionGet();
        assertThat(flushResponse.getTotalShards(), equalTo(numShards.totalNumShards));
        assertThat(flushResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
        assertThat(flushResponse.getFailedShards(), equalTo(0));
        client().index(indexRequest("test").type("type1").id("2").source(source("2", "test"))).actionGet();
        RefreshResponse refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(numShards.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        allowNodes("test", 2);

        logger.info("Running Cluster Health");
        ensureGreen();

        GetResponse getResult;

        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").type("type1").id("1").operationThreaded(false)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client().get(getRequest("test").type("type1").id("1").operationThreaded(false)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client().get(getRequest("test").type("type1").id("2").operationThreaded(true)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
            getResult = client().get(getRequest("test").type("type1").id("2").operationThreaded(true)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
        }

        // now start another one so we move some primaries
        allowNodes("test", 3);
        Thread.sleep(200);
        logger.info("Running Cluster Health");
        ensureGreen();

        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client().get(getRequest("test").type("type1").id("1")).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("1", "test")));
            getResult = client().get(getRequest("test").type("type1").id("2").operationThreaded(true)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
            getResult = client().get(getRequest("test").type("type1").id("2").operationThreaded(true)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
            getResult = client().get(getRequest("test").type("type1").id("2").operationThreaded(true)).actionGet();
            assertThat(getResult.getSourceAsString(), equalTo(source("2", "test")));
        }
    }

    private String source(String id, String nameValue) {
        return "{ \"type1\" : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
