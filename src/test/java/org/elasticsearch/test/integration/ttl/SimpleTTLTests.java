/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.ttl;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class SimpleTTLTests extends AbstractNodesTests {

    static private final long purgeInterval = 200;
    private Client client;

    @BeforeClass public void createNodes() throws Exception {
        Settings settings = settingsBuilder().put("indices.ttl.interval", purgeInterval).build();
        startNode("node1", settings);
        startNode("node2", settings);
        client = getClient();
    }

    @AfterClass public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test public void testSimpleTTL() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .addMapping("type1", XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type1")
                        .startObject("_timestamp").field("enabled", true).field("store", "yes").endObject()
                        .startObject("_ttl").field("enabled", true).field("store", "yes").endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        long providedTTLValue = 3000;
        logger.info("--> checking ttl");
        client.prepareIndex("test", "type1", "1").setSource("field1", "value1").setTTL(providedTTLValue).setRefresh(true).execute().actionGet();
        long now = System.currentTimeMillis();

        // realtime get check
        long now1 = System.currentTimeMillis();
        GetResponse getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        long ttl0 = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl0, greaterThan(0L));
        assertThat(ttl0, lessThan(providedTTLValue - (now1 - now)));
        // verify the ttl is still decreasing when going to the replica
        now1 = System.currentTimeMillis();
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        ttl0 = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl0, greaterThan(0L));
        assertThat(ttl0, lessThan(providedTTLValue - (now1 - now)));
        // non realtime get (stored)
        now1 = System.currentTimeMillis();
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        ttl0 = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl0, greaterThan(0L));
        assertThat(ttl0, lessThan(providedTTLValue - (now1 - now)));
        // non realtime get going the replica
        now1 = System.currentTimeMillis();
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        ttl0 = ((Number) getResponse.field("_ttl").value()).longValue();
        assertThat(ttl0, greaterThan(0L));
        assertThat(ttl0, lessThan(providedTTLValue - (now1 - now)));

        logger.info("--> checking purger");
        // make sure the purger has done its job
        long shouldBeExpiredDate = now + providedTTLValue + purgeInterval + 2000;
        now1 = System.currentTimeMillis();
        if (shouldBeExpiredDate - now1 > 0) {
            Thread.sleep(shouldBeExpiredDate - now1);
        }
        // realtime get check
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.exists(), equalTo(false));
        // replica realtime get check
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(true).execute().actionGet();
        assertThat(getResponse.exists(), equalTo(false));
        // non realtime get (stored) check
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        assertThat(getResponse.exists(), equalTo(false));
        // non realtime get going the replica check
        getResponse = client.prepareGet("test", "type1", "1").setFields("_ttl").setRealtime(false).execute().actionGet();
        assertThat(getResponse.exists(), equalTo(false));
    }
}
