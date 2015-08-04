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
package org.elasticsearch.percolator;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.percolator.PercolatorIT.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertMatchCount;
import static org.hamcrest.Matchers.*;

/**
 */
public class MultiPercolatorIT extends ESIntegTestCase {

    @Test
    public void testBasics() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "field1", "type=string"));
        ensureGreen();

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
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
        assertMatchCount(item.getResponse(), 2l);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getErrorMessage(), nullValue());
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        item = response.getItems()[1];
        assertThat(item.getErrorMessage(), nullValue());

        assertMatchCount(item.getResponse(), 2l);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        item = response.getItems()[2];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 4l);
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        item = response.getItems()[3];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 1l);
        assertThat(item.getResponse().getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContaining("4"));

        item = response.getItems()[4];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.getErrorMessage(), notNullValue());
        assertThat(item.getErrorMessage(), containsString("document missing"));
    }

    @Test
    public void testWithRouting() throws Exception {
        assertAcked(prepareCreate("test").addMapping("type", "field1", "type=string"));
        ensureGreen();

        logger.info("--> register a queries");
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "1")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "2")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "3")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                                .must(matchQuery("field1", "b"))
                                .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorService.TYPE_NAME, "4")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();

        MultiPercolateResponse response = client().prepareMultiPercolate()
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject())))
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject())))
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject())))
                .add(client().preparePercolate()
                        .setIndices("test").setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject())))
                .add(client().preparePercolate() // non existing doc, so error element
                        .setIndices("test").setDocumentType("type")
                        .setRouting("a")
                        .setGetRequest(Requests.getRequest("test").type("type").id("5")))
                .execute().actionGet();

        MultiPercolateResponse.Item item = response.getItems()[0];
        assertMatchCount(item.getResponse(), 2l);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getErrorMessage(), nullValue());
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "4"));

        item = response.getItems()[1];
        assertThat(item.getErrorMessage(), nullValue());

        assertMatchCount(item.getResponse(), 2l);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        item = response.getItems()[2];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 4l);
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        item = response.getItems()[3];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 1l);
        assertThat(item.getResponse().getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContaining("4"));

        item = response.getItems()[4];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.getErrorMessage(), notNullValue());
        assertThat(item.getErrorMessage(), containsString("document missing"));
    }

    @Test
    public void testExistingDocsOnly() throws Exception {
        createIndex("test");

        int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
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
            assertMatchCount(item.getResponse(), numQueries);
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
            assertThat(item.getErrorMessage(), containsString("document missing"));
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
        assertMatchCount(response.items()[numPercolateRequest].getResponse(), numQueries);
        assertThat(response.items()[numPercolateRequest].getResponse().getMatches().length, equalTo(numQueries));
    }

    @Test
    public void testWithDocsOnly() throws Exception {
        createIndex("test");
        ensureGreen();

        NumShards test = getNumShards("test");

        int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client().prepareIndex("test", PercolatorService.TYPE_NAME, Integer.toString(i))
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
            assertMatchCount(item.getResponse(), numQueries);
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
            assertThat(item.getResponse().getShardFailures().length, equalTo(test.numPrimaries));
            for (ShardOperationFailedException shardFailure : item.getResponse().getShardFailures()) {
                assertThat(shardFailure.reason(), containsString("Failed to derive xcontent"));
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
        assertMatchCount(response.items()[numPercolateRequest].getResponse(), numQueries);
        assertThat(response.items()[numPercolateRequest].getResponse().getMatches().length, equalTo(numQueries));
    }


    @Test
    public void testNestedMultiPercolation() throws IOException {
        initNestedIndexAndPercolation();
        MultiPercolateRequestBuilder mpercolate= client().prepareMultiPercolate();
        mpercolate.add(client().preparePercolate().setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getNotMatchingNestedDoc())).setIndices("nestedindex").setDocumentType("company"));
        mpercolate.add(client().preparePercolate().setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getMatchingNestedDoc())).setIndices("nestedindex").setDocumentType("company"));
        MultiPercolateResponse response = mpercolate.get();
        assertEquals(response.getItems()[0].getResponse().getMatches().length, 0);
        assertEquals(response.getItems()[1].getResponse().getMatches().length, 1);
        assertEquals(response.getItems()[1].getResponse().getMatches()[0].getId().string(), "Q");
    }

    void initNestedIndexAndPercolation() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("companyname").field("type", "string").endObject()
                .startObject("employee").field("type", "nested").startObject("properties")
                .startObject("name").field("type", "string").endObject().endObject().endObject().endObject()
                .endObject();

        assertAcked(client().admin().indices().prepareCreate("nestedindex").addMapping("company", mapping));
        ensureGreen("nestedindex");

        client().prepareIndex("nestedindex", PercolatorService.TYPE_NAME, "Q").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee", QueryBuilders.matchQuery("employee.name", "virginia potts").operator(MatchQueryBuilder.Operator.AND)).scoreMode("avg")).endObject()).get();

        refresh();

    }

    XContentBuilder getMatchingNestedDoc() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject().field("companyname", "stark").startArray("employee")
                .startObject().field("name", "virginia potts").endObject()
                .startObject().field("name", "tony stark").endObject()
                .endArray().endObject();
        return doc;
    }

    XContentBuilder getNotMatchingNestedDoc() throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject().field("companyname", "notstark").startArray("employee")
                .startObject().field("name", "virginia stark").endObject()
                .startObject().field("name", "tony potts").endObject()
                .endArray().endObject();
        return doc;
    }

}
