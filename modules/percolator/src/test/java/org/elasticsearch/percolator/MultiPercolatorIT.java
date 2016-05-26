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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.percolator.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.yamlBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.percolator.PercolatorTestUtil.convertFromTextArray;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.percolator.PercolatorTestUtil.assertMatchCount;
import static org.elasticsearch.percolator.PercolatorTestUtil.preparePercolate;
import static org.elasticsearch.percolator.PercolatorTestUtil.prepareMultiPercolate;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class MultiPercolatorIT extends ESIntegTestCase {

    private final static String INDEX_NAME = "queries";
    private final static String TYPE_NAME = "query";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    public void testBasics() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("type", "field1", "type=text"));
        ensureGreen();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        MultiPercolateResponse response = prepareMultiPercolate(client())
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject())))
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject())))
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject())))
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject())))
                .add(preparePercolate(client()) // non existing doc, so error element
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("5")))
                .execute().actionGet();

        MultiPercolateResponse.Item item = response.getItems()[0];
        assertMatchCount(item.getResponse(), 2L);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getErrorMessage(), nullValue());
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "4"));

        item = response.getItems()[1];
        assertThat(item.getErrorMessage(), nullValue());

        assertMatchCount(item.getResponse(), 2L);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));

        item = response.getItems()[2];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 4L);
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4"));

        item = response.getItems()[3];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 1L);
        assertThat(item.getResponse().getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContaining("4"));

        item = response.getItems()[4];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.getErrorMessage(), notNullValue());
        assertThat(item.getErrorMessage(), containsString("[" + INDEX_NAME + "/type/5] doesn't exist"));
    }

    public void testWithRouting() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("type", "field1", "type=text"));
        ensureGreen();

        logger.info("--> register a queries");
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "1")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "2")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "3")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                                .must(matchQuery("field1", "b"))
                                .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex(INDEX_NAME, TYPE_NAME, "4")
                .setRouting("a")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        refresh();

        MultiPercolateResponse response = prepareMultiPercolate(client())
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "b").endObject())))
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(yamlBuilder().startObject().field("field1", "c").endObject())))
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(smileBuilder().startObject().field("field1", "b c").endObject())))
                .add(preparePercolate(client())
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setRouting("a")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field1", "d").endObject())))
                .add(preparePercolate(client()) // non existing doc, so error element
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setRouting("a")
                        .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("5")))
                .execute().actionGet();

        MultiPercolateResponse.Item item = response.getItems()[0];
        assertMatchCount(item.getResponse(), 2L);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getErrorMessage(), nullValue());
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "4"));

        item = response.getItems()[1];
        assertThat(item.getErrorMessage(), nullValue());

        assertMatchCount(item.getResponse(), 2L);
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContainingInAnyOrder("2", "4"));

        item = response.getItems()[2];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 4L);
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContainingInAnyOrder("1", "2", "3", "4"));

        item = response.getItems()[3];
        assertThat(item.getErrorMessage(), nullValue());
        assertMatchCount(item.getResponse(), 1L);
        assertThat(item.getResponse().getMatches(), arrayWithSize(1));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), INDEX_NAME), arrayContaining("4"));

        item = response.getItems()[4];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.getErrorMessage(), notNullValue());
        assertThat(item.getErrorMessage(), containsString("[" + INDEX_NAME + "/type/5] doesn't exist"));
    }

    public void testExistingDocsOnly() throws Exception {
        prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, "query", "type=percolator").get();

        int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client().prepareIndex(INDEX_NAME, TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }

        client().prepareIndex(INDEX_NAME, "type", "1")
                .setSource(jsonBuilder().startObject().field("field", "a").endObject())
                .execute().actionGet();
        refresh();

        MultiPercolateRequestBuilder builder = prepareMultiPercolate(client());
        int numPercolateRequest = randomIntBetween(50, 100);
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    preparePercolate(client())
                            .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("1"))
                            .setIndices(INDEX_NAME).setDocumentType("type")
                            .setSize(numQueries)
            );
        }

        MultiPercolateResponse response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(false));
            assertMatchCount(item.getResponse(), numQueries);
            assertThat(item.getResponse().getMatches().length, equalTo(numQueries));
        }

        // Non existing doc
        builder = prepareMultiPercolate(client());
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    preparePercolate(client())
                            .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2"))
                            .setIndices(INDEX_NAME).setDocumentType("type").setSize(numQueries)

            );
        }

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(true));
            assertThat(item.getErrorMessage(), containsString("doesn't exist"));
            assertThat(item.getResponse(), nullValue());
        }

        // One existing doc
        builder = prepareMultiPercolate(client());
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    preparePercolate(client())
                            .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("2"))
                            .setIndices(INDEX_NAME).setDocumentType("type").setSize(numQueries)
            );
        }
        builder.add(
                preparePercolate(client())
                        .setGetRequest(Requests.getRequest(INDEX_NAME).type("type").id("1"))
                        .setIndices(INDEX_NAME).setDocumentType("type").setSize(numQueries)
        );

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest + 1));
        assertThat(response.items()[numPercolateRequest].isFailure(), equalTo(false));
        assertMatchCount(response.items()[numPercolateRequest].getResponse(), numQueries);
        assertThat(response.items()[numPercolateRequest].getResponse().getMatches().length, equalTo(numQueries));
    }

    public void testWithDocsOnly() throws Exception {
        prepareCreate(INDEX_NAME).addMapping(TYPE_NAME, "query", "type=percolator").get();
        ensureGreen();

        int numQueries = randomIntBetween(50, 100);
        logger.info("--> register a queries");
        for (int i = 0; i < numQueries; i++) {
            client().prepareIndex(INDEX_NAME, TYPE_NAME, Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .execute().actionGet();
        }
        refresh();

        MultiPercolateRequestBuilder builder = prepareMultiPercolate(client());
        int numPercolateRequest = randomIntBetween(50, 100);
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    preparePercolate(client())
                            .setIndices(INDEX_NAME).setDocumentType("type")
                            .setSize(numQueries)
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
        builder = prepareMultiPercolate(client());
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    preparePercolate(client())
                            .setIndices(INDEX_NAME).setDocumentType("type")
                            .setSource("illegal json"));
        }

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest));
        for (MultiPercolateResponse.Item item : response) {
            assertThat(item.isFailure(), equalTo(true));
            assertThat(item.getFailure(), notNullValue());
        }

        // one valid request
        builder = prepareMultiPercolate(client());
        for (int i = 0; i < numPercolateRequest; i++) {
            builder.add(
                    preparePercolate(client())
                            .setIndices(INDEX_NAME).setDocumentType("type")
                            .setSource("illegal json"));
        }
        builder.add(
                preparePercolate(client())
                        .setSize(numQueries)
                        .setIndices(INDEX_NAME).setDocumentType("type")
                        .setPercolateDoc(docBuilder().setDoc(jsonBuilder().startObject().field("field", "a").endObject())));

        response = builder.execute().actionGet();
        assertThat(response.items().length, equalTo(numPercolateRequest + 1));
        assertThat(response.items()[numPercolateRequest].isFailure(), equalTo(false));
        assertMatchCount(response.items()[numPercolateRequest].getResponse(), numQueries);
        assertThat(response.items()[numPercolateRequest].getResponse().getMatches().length, equalTo(numQueries));
    }

    public void testNestedMultiPercolation() throws IOException {
        initNestedIndexAndPercolation();
        MultiPercolateRequestBuilder mpercolate= prepareMultiPercolate(client());
        mpercolate.add(preparePercolate(client()).setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getNotMatchingNestedDoc())).setIndices(INDEX_NAME).setDocumentType("company"));
        mpercolate.add(preparePercolate(client()).setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc(getMatchingNestedDoc())).setIndices(INDEX_NAME).setDocumentType("company"));
        MultiPercolateResponse response = mpercolate.get();
        assertEquals(response.getItems()[0].getResponse().getMatches().length, 0);
        assertEquals(response.getItems()[1].getResponse().getMatches().length, 1);
        assertEquals(response.getItems()[1].getResponse().getMatches()[0].getId().string(), "Q");
    }

    void initNestedIndexAndPercolation() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject().startObject("properties").startObject("companyname").field("type", "text").endObject()
                .startObject("employee").field("type", "nested").startObject("properties")
                .startObject("name").field("type", "text").endObject().endObject().endObject().endObject()
                .endObject();

        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME)
                .addMapping(TYPE_NAME, "query", "type=percolator")
                .addMapping("company", mapping));
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME, TYPE_NAME, "Q").setSource(jsonBuilder().startObject()
                .field("query", QueryBuilders.nestedQuery("employee", QueryBuilders.matchQuery("employee.name", "virginia potts").operator(Operator.AND), ScoreMode.Avg)).endObject()).get();

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
