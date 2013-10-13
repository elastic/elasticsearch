/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.percolator;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.percolator.PercolatorTests.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 */
public class MultiPercolatorTests extends AbstractIntegrationTest {

    @Test
    public void testBasics() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> register a queries");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();

        MultiPercolateResponse response = client().prepareMultiPercolate()
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject())))
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject())))
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject())))
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject())))
                .add(client().preparePercolate() // non existing doc, so error element
                        .setIndices("test").setDocumentType("type")
                        .setGetRequest(Requests.getRequest("test").type("type").id("5")))
                .execute().actionGet();

        MultiPercolateResponse.Item item = response.getItems()[0];
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.errorMessage(), nullValue());
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        item = response.getItems()[1];
        assertThat(item.errorMessage(), nullValue());

        assertNoFailures(item.response());
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getResponse().getCount(), equalTo(2l));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        item =  response.getItems()[2];
        assertThat(item.errorMessage(), nullValue());
        assertNoFailures(item.response());
        assertThat(item.getResponse().getMatches(), arrayWithSize(4));
        assertThat(item.getResponse().getCount(), equalTo(4l));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        item = response.getItems()[3];
        assertThat(item.errorMessage(), nullValue());
        assertNoFailures(item.response());
        assertThat(item.getResponse().getMatches(), arrayWithSize(1));
        assertThat(item.getResponse().getCount(), equalTo(1l));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContaining("4"));

        item = response.getItems()[4];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.errorMessage(), notNullValue());
        assertThat(item.errorMessage(), containsString("document missing"));
    }

    @Test
    public void testExistingDocsOnly() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 1)
                                .build())
                .execute().actionGet();
        ensureGreen();

        int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client().prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }

        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("field", "a"))
                .execute().actionGet();

        MultiPercolateRequestBuilder builder = client().prepareMultiPercolate();
        int numPercolateRequest = randomIntBetween(50, 100);
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    client().preparePercolate()
                            .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                            .setIndices("test").setDocumentType("type"));
        }

        MultiPercolateResponse response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(false));
            assertNoFailures(item.response());
            assertThat(item.getResponse().getCount(), equalTo((long) numQueries));
            assertThat(item.getResponse().getMatches().length, equalTo(numQueries));
        }

        // Non existing doc
        builder = client().prepareMultiPercolate();
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    client().preparePercolate()
                            .setGetRequest(Requests.getRequest("test").type("type").id("2"))
                            .setIndices("test").setDocumentType("type"));
        }

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(true));
            assertThat(item.errorMessage(), containsString("document missing"));
            assertThat(item.getResponse(), nullValue());
        }

        // One existing doc
        builder = client().prepareMultiPercolate();
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    client().preparePercolate()
                            .setGetRequest(Requests.getRequest("test").type("type").id("2"))
                            .setIndices("test").setDocumentType("type"));
        }
        builder.add(
                client().preparePercolate()
                        .setGetRequest(Requests.getRequest("test").type("type").id("1"))
                        .setIndices("test").setDocumentType("type"));

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest + 1));
        assertThat(response.items()[numPercolateRequest].isFailure(), equalTo(false));
        assertNoFailures(response.items()[numPercolateRequest].response());
        assertThat(response.items()[numPercolateRequest].getResponse().getCount(), equalTo((long) numQueries));
        assertThat(response.items()[numPercolateRequest].getResponse().getMatches().length, equalTo(numQueries));
    }

    @Test
    public void testWithDocsOnly() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 1)
                                .build())
                .execute().actionGet();
        ensureGreen();

        int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client().prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }

        MultiPercolateRequestBuilder builder = client().prepareMultiPercolate();
        int numPercolateRequest = randomIntBetween(50, 100);
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    client().preparePercolate()
                    .setIndices("test").setDocumentType("type")
                    .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field", "a").endObject())));
        }

        MultiPercolateResponse response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(false));
            assertNoFailures(item.response());
            assertThat(item.getResponse().getCount(), equalTo((long) numQueries));
            assertThat(item.getResponse().getMatches().length, equalTo(numQueries));
        }

        // All illegal json
        builder = client().prepareMultiPercolate();
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    client().preparePercolate()
                            .setIndices("test").setDocumentType("type")
                            .setSource("illegal json"));
        }

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(false));
            assertThat(item.getResponse().getSuccessfulShards(), equalTo(0));
            assertThat(item.getResponse().getShardFailures().length, equalTo(2));
            for (ShardOperationFailedException shardFailure : item.getResponse().getShardFailures()) {
                assertThat(shardFailure.reason(), containsString("Failed to derive xcontent from"));
                assertThat(shardFailure.status().getStatus(), equalTo(500));
            }
        }

        // one valid request
        builder = client().prepareMultiPercolate();
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    client().preparePercolate()
                            .setIndices("test").setDocumentType("type")
                            .setSource("illegal json"));
        }
        builder.add(
                client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field", "a").endObject())));

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest + 1));
        assertThat(response.items()[numPercolateRequest].isFailure(), equalTo(false));
        assertNoFailures(response.items()[numPercolateRequest].getResponse());
        assertThat(response.items()[numPercolateRequest].getResponse().getCount(), equalTo((long ) numQueries));
        assertThat(response.items()[numPercolateRequest].getResponse().getMatches().length, equalTo(numQueries));
    }

}
