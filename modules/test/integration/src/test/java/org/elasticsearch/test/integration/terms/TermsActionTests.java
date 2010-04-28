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

package org.elasticsearch.test.integration.terms;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.action.terms.TermsRequest;
import org.elasticsearch.action.terms.TermsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.action.terms.TermsRequest.SortType.*;
import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.util.MapBuilder.*;
import static org.elasticsearch.util.xcontent.XContentFactory.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class TermsActionTests extends AbstractNodesTests {

    private Client client;

    @BeforeMethod public void createNodesAndClient() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();

        logger.info("Creating index test");
        client.admin().indices().create(createIndexRequest("test")).actionGet();
        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client.admin().cluster().health(clusterHealth().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));
    }

    @AfterMethod public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server2");
    }

    @Test public void testSimpleStringTerms() throws Exception {
        IndexStatus indexStatus = client.admin().indices().status(indicesStatus("test")).actionGet().index("test");

        // verify no freqs
        logger.info("Verify no freqs");
        TermsResponse termsResponse = client.terms(termsRequest("test").fields("value")).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat("no term freqs for the 'value' since nothing is indexed", termsResponse.field("value").iterator().hasNext(), equalTo(false));

        logger.info("Index [1]");
        client.index(indexRequest("test").type("type1").id("1").source(jsonBuilder().startObject().field("value", "aaa").endObject())).actionGet();
        logger.info("Refresh");
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Verify freqs");
        termsResponse = client.terms(termsRequest("test").fields("value")).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.numDocs(), equalTo(1l));
        assertThat(termsResponse.maxDoc(), equalTo(1l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(-1));

        logger.info("Index [2]");
        client.index(indexRequest("test").type("type1").id("2").source(jsonBuilder().startObject().field("value", "bbb bbb").endObject())).actionGet();
        logger.info("Refresh");
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Verify freqs");
        termsResponse = client.terms(termsRequest("test").fields("value")).actionGet();
        assertThat(termsResponse.numDocs(), equalTo(2l));
        assertThat(termsResponse.maxDoc(), equalTo(2l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(1));

        logger.info("Verify freqs (no fields, on _all)");
        termsResponse = client.terms(termsRequest("test")).actionGet();
        assertThat(termsResponse.numDocs(), equalTo(2l));
        assertThat(termsResponse.maxDoc(), equalTo(2l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("_all").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("_all").docFreq("bbb"), equalTo(1));

        logger.info("Delete 3");
        client.index(indexRequest("test").type("type1").id("3").source(jsonBuilder().startObject().field("value", "bbb").endObject())).actionGet();
        logger.info("Refresh");
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Verify freqs");
        termsResponse = client.terms(termsRequest("test").fields("value")).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(3l));
        assertThat(termsResponse.maxDoc(), equalTo(3l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(2));

        logger.info("Verify freqs (sort gy freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(FREQ)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[1].termAsString(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(1));

        logger.info("Verify freq (size and sort by freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(FREQ).size(1)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(-1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));

        logger.info("Verify freq (minFreq with sort by freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(FREQ).minFreq(2)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(-1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));

        logger.info("Verify freq (prefix with sort by freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(FREQ).prefix("bb")).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(-1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));

        // test deleting the last doc
        logger.info("Delete [3]");
        client.delete(deleteRequest("test").type("type1").id("3")).actionGet();
        logger.info("Refresh");
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Verify freq (even after refresh, won't see the delete)");
        termsResponse = client.terms(termsRequest("test").fields("value")).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(2l));
        assertThat(termsResponse.maxDoc(), equalTo(3l));
        assertThat(termsResponse.deletedDocs(), equalTo(1l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(2));

        logger.info("Verify freq (with exact, should see the delete)");
        termsResponse = client.terms(termsRequest("test").fields("value").exact(true)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(1));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(1));

        logger.info("Optimize (onlyExpungeDeletes with refresh)");
        OptimizeResponse optimizeResponse = client.admin().indices().optimize(optimizeRequest("test").onlyExpungeDeletes(true).refresh(true)).actionGet();

        logger.info("Verify freq (we will see the delete now, without exact)");
        termsResponse = client.terms(termsRequest("test").fields("value").exact(false)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(2l));
        assertThat(termsResponse.maxDoc(), equalTo(2l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(1));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[0].termAsString(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].termAsString(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(1));
    }

    @Test public void testNumberedTerms() throws Exception {
        IndexStatus indexStatus = client.admin().indices().status(indicesStatus("test")).actionGet().index("test");

        logger.info("Index ...");
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 1).put("fl", 2.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 1).put("fl", 2.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 1).put("fl", 2.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 1).put("fl", 2.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 2).put("fl", 3.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 2).put("fl", 3.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 2).put("fl", 3.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 3).put("fl", 4.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 11).put("fl", 12.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 11).put("fl", 12.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 12).put("fl", 13.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 12).put("fl", 13.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 12).put("fl", 13.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 13).put("fl", 14.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 13).put("fl", 14.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 21).put("fl", 20.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 22).put("fl", 21.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 22).put("fl", 21.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 22).put("fl", 21.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 22).put("fl", 21.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 22).put("fl", 21.0f).map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 22).put("fl", 21.0f).map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("int", 23).put("fl", 22.0f).map())).actionGet();

        logger.info("Refresh");
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Verify int with sort on term");
        TermsResponse termsResponse = client.terms(termsRequest("test").fields("int").sortType(TermsRequest.SortType.TERM)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(23l));
        assertThat(termsResponse.maxDoc(), equalTo(23l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("int").docFreq(1), equalTo(4));
        assertThat(termsResponse.field("int").docFreq(2), equalTo(3));
        // check the order
        assertThat(termsResponse.field("int").termsFreqs().length, equalTo(9));
        assertThat(termsResponse.field("int").termsFreqs()[0].termAsString(), equalTo("1"));
        assertThat(termsResponse.field("int").termsFreqs()[0].docFreq(), equalTo(4));
        assertThat(termsResponse.field("int").termsFreqs()[1].termAsString(), equalTo("2"));
        assertThat(termsResponse.field("int").termsFreqs()[1].docFreq(), equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[2].termAsString(), equalTo("3"));
        assertThat(termsResponse.field("int").termsFreqs()[2].docFreq(), equalTo(1));
        assertThat(termsResponse.field("int").termsFreqs()[3].termAsString(), equalTo("11"));
        assertThat(termsResponse.field("int").termsFreqs()[3].docFreq(), equalTo(2));
        assertThat(termsResponse.field("int").termsFreqs()[4].termAsString(), equalTo("12"));
        assertThat(termsResponse.field("int").termsFreqs()[4].docFreq(), equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[5].termAsString(), equalTo("13"));
        assertThat(termsResponse.field("int").termsFreqs()[5].docFreq(), equalTo(2));
        assertThat(termsResponse.field("int").termsFreqs()[6].termAsString(), equalTo("21"));
        assertThat(termsResponse.field("int").termsFreqs()[6].docFreq(), equalTo(1));

        logger.info("Verify int with sort on freq");
        termsResponse = client.terms(termsRequest("test").fields("int").sortType(TermsRequest.SortType.FREQ)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(23l));
        assertThat(termsResponse.maxDoc(), equalTo(23l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("int").docFreq(1), equalTo(4));
        assertThat(termsResponse.field("int").docFreq(2), equalTo(3));

        assertThat(termsResponse.field("int").termsFreqs().length, equalTo(9));
        assertThat(termsResponse.field("int").termsFreqs()[0].termAsString(), equalTo("22"));
        assertThat(termsResponse.field("int").termsFreqs()[0].docFreq(), equalTo(6));
        assertThat(termsResponse.field("int").termsFreqs()[1].termAsString(), equalTo("1"));
        assertThat(termsResponse.field("int").termsFreqs()[1].docFreq(), equalTo(4));

        logger.info("Verify int with sort on freq and from 2 to 11");
        termsResponse = client.terms(termsRequest("test").fields("int").sortType(TermsRequest.SortType.FREQ).from(2).to(11)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(23l));
        assertThat(termsResponse.maxDoc(), equalTo(23l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("int").docFreq(1), equalTo(-1));
        assertThat(termsResponse.field("int").docFreq(2), equalTo(3));

        assertThat(termsResponse.field("int").termsFreqs().length, equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[0].termAsString(), equalTo("2"));
        assertThat(termsResponse.field("int").termsFreqs()[0].docFreq(), equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[1].termAsString(), equalTo("11"));
        assertThat(termsResponse.field("int").termsFreqs()[1].docFreq(), equalTo(2));
        assertThat(termsResponse.field("int").termsFreqs()[2].termAsString(), equalTo("3"));
        assertThat(termsResponse.field("int").termsFreqs()[2].docFreq(), equalTo(1));

        logger.info("Verify int with sort on term and from 2 to 11");
        termsResponse = client.terms(termsRequest("test").fields("int").sortType(TermsRequest.SortType.TERM).from(2).to(11)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(23l));
        assertThat(termsResponse.maxDoc(), equalTo(23l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("int").docFreq(1), equalTo(-1));
        assertThat(termsResponse.field("int").docFreq(2), equalTo(3));

        assertThat(termsResponse.field("int").termsFreqs().length, equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[0].termAsString(), equalTo("2"));
        assertThat(termsResponse.field("int").termsFreqs()[0].docFreq(), equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[1].termAsString(), equalTo("3"));
        assertThat(termsResponse.field("int").termsFreqs()[1].docFreq(), equalTo(1));
        assertThat(termsResponse.field("int").termsFreqs()[2].termAsString(), equalTo("11"));
        assertThat(termsResponse.field("int").termsFreqs()[2].docFreq(), equalTo(2));

        logger.info("Verify int with sort on term and from 2 to 11, fromInclusive=false");
        termsResponse = client.terms(termsRequest("test").fields("int").sortType(TermsRequest.SortType.TERM).from(2).to(11).fromInclusive(false)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(23l));
        assertThat(termsResponse.maxDoc(), equalTo(23l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("int").docFreq(1), equalTo(-1));
        assertThat(termsResponse.field("int").docFreq(3), equalTo(1));

        assertThat(termsResponse.field("int").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("int").termsFreqs()[0].termAsString(), equalTo("3"));
        assertThat(termsResponse.field("int").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("int").termsFreqs()[1].termAsString(), equalTo("11"));
        assertThat(termsResponse.field("int").termsFreqs()[1].docFreq(), equalTo(2));

        logger.info("Verify int with sort on term and from 2 to 11, toInclusive=false");
        termsResponse = client.terms(termsRequest("test").fields("int").sortType(TermsRequest.SortType.TERM).from(2).to(11).toInclusive(false)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(23l));
        assertThat(termsResponse.maxDoc(), equalTo(23l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("int").docFreq(1), equalTo(-1));
        assertThat(termsResponse.field("int").docFreq(2), equalTo(3));

        assertThat(termsResponse.field("int").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("int").termsFreqs()[0].termAsString(), equalTo("2"));
        assertThat(termsResponse.field("int").termsFreqs()[0].docFreq(), equalTo(3));
        assertThat(termsResponse.field("int").termsFreqs()[1].termAsString(), equalTo("3"));
        assertThat(termsResponse.field("int").termsFreqs()[1].docFreq(), equalTo(1));
    }

    @Test public void testDateTerms() throws Exception {
        IndexStatus indexStatus = client.admin().indices().status(indicesStatus("test")).actionGet().index("test");

        logger.info("Index ...");
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("date", "2003-01-01").map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("date", "2003-01-01").map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("date", "2003-01-02").map())).actionGet();

        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("date", "2003-01-03").map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("date", "2003-01-03").map())).actionGet();
        client.index(indexRequest("test").type("type1").source(newMapBuilder().put("date", "2003-01-03").map())).actionGet();

        logger.info("Refresh");
        client.admin().indices().refresh(refreshRequest()).actionGet();

        logger.info("Verify int with sort on term");
        TermsResponse termsResponse = client.terms(termsRequest("test").fields("date").sortType(TermsRequest.SortType.TERM)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(6l));
        assertThat(termsResponse.maxDoc(), equalTo(6l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("date").docFreq("2003-01-01T00:00:00.000Z"), equalTo(2));
        assertThat(termsResponse.field("date").docFreq("2003-01-02T00:00:00.000Z"), equalTo(1));
        assertThat(termsResponse.field("date").docFreq("2003-01-03T00:00:00.000Z"), equalTo(3));

        assertThat(termsResponse.field("date").termsFreqs().length, equalTo(3));
        assertThat(termsResponse.field("date").termsFreqs()[0].termAsString(), equalTo("2003-01-01T00:00:00.000Z"));
        assertThat(termsResponse.field("date").termsFreqs()[0].docFreq(), equalTo(2));
        assertThat(termsResponse.field("date").termsFreqs()[1].termAsString(), equalTo("2003-01-02T00:00:00.000Z"));
        assertThat(termsResponse.field("date").termsFreqs()[1].docFreq(), equalTo(1));
        assertThat(termsResponse.field("date").termsFreqs()[2].termAsString(), equalTo("2003-01-03T00:00:00.000Z"));
        assertThat(termsResponse.field("date").termsFreqs()[2].docFreq(), equalTo(3));

        logger.info("Verify int with sort on freq");
        termsResponse = client.terms(termsRequest("test").fields("date").sortType(TermsRequest.SortType.FREQ)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.numDocs(), equalTo(6l));
        assertThat(termsResponse.maxDoc(), equalTo(6l));
        assertThat(termsResponse.deletedDocs(), equalTo(0l));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("date").docFreq("2003-01-01T00:00:00.000Z"), equalTo(2));
        assertThat(termsResponse.field("date").docFreq("2003-01-02T00:00:00.000Z"), equalTo(1));
        assertThat(termsResponse.field("date").docFreq("2003-01-03T00:00:00.000Z"), equalTo(3));

        assertThat(termsResponse.field("date").termsFreqs().length, equalTo(3));
        assertThat(termsResponse.field("date").termsFreqs()[0].termAsString(), equalTo("2003-01-03T00:00:00.000Z"));
        assertThat(termsResponse.field("date").termsFreqs()[0].docFreq(), equalTo(3));
        assertThat(termsResponse.field("date").termsFreqs()[1].termAsString(), equalTo("2003-01-01T00:00:00.000Z"));
        assertThat(termsResponse.field("date").termsFreqs()[1].docFreq(), equalTo(2));
        assertThat(termsResponse.field("date").termsFreqs()[2].termAsString(), equalTo("2003-01-02T00:00:00.000Z"));
        assertThat(termsResponse.field("date").termsFreqs()[2].docFreq(), equalTo(1));
    }
}
