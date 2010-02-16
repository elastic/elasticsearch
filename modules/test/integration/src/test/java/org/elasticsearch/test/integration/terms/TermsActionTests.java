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

import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.action.terms.TermsRequest;
import org.elasticsearch.action.terms.TermsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractServersTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.util.json.JsonBuilder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
@Test
public class TermsActionTests extends AbstractServersTests {

    @AfterMethod public void closeServers() {
        closeAllServers();
    }

    @Test public void testTermsAction() throws Exception {
        startServer("server1");
        startServer("server2");
        Client client = getClient();
        try {
            verifyTermsActions(client);
        } finally {
            client.close();
        }
    }

    protected Client getClient() {
        return client("server2");
    }

    protected void verifyTermsActions(Client client) throws Exception {
        logger.info("Creating index test");
        client.admin().indices().create(createIndexRequest("test")).actionGet();
        Thread.sleep(500);

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
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].term(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(2));

        logger.info("Verify freqs (sort gy freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(TermsRequest.SortType.FREQ)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));
        assertThat(termsResponse.field("value").termsFreqs()[1].term(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(1));

        logger.info("Verify freq (size and sort by freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(TermsRequest.SortType.FREQ).size(1)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(-1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));

        logger.info("Verify freq (minFreq with sort by freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(TermsRequest.SortType.FREQ).minFreq(2)).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(-1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(2));

        logger.info("Verify freq (prefix with sort by freq)");
        termsResponse = client.terms(termsRequest("test").fields("value").sortType(TermsRequest.SortType.FREQ).prefix("bb")).actionGet();
        assertThat(termsResponse.successfulShards(), equalTo(indexStatus.shards().size()));
        assertThat(termsResponse.failedShards(), equalTo(0));
        assertThat(termsResponse.fieldsAsMap().isEmpty(), equalTo(false));
        assertThat(termsResponse.field("value").docFreq("aaa"), equalTo(-1));
        assertThat(termsResponse.field("value").docFreq("bbb"), equalTo(2));
        // check the order
        assertThat(termsResponse.field("value").termsFreqs().length, equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("bbb"));
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
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].term(), equalTo("bbb"));
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
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].term(), equalTo("bbb"));
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
        assertThat(termsResponse.field("value").termsFreqs()[0].term(), equalTo("aaa"));
        assertThat(termsResponse.field("value").termsFreqs()[0].docFreq(), equalTo(1));
        assertThat(termsResponse.field("value").termsFreqs()[1].term(), equalTo("bbb"));
        assertThat(termsResponse.field("value").termsFreqs()[1].docFreq(), equalTo(1));
    }
}
