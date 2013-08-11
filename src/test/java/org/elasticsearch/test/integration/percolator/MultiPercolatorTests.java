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

package org.elasticsearch.test.integration.percolator;

import org.elasticsearch.action.percolate.MultiPercolateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.integration.percolator.SimplePercolatorTests.convertFromTextArray;
import static org.hamcrest.Matchers.*;

/**
 */
public class MultiPercolatorTests extends AbstractSharedClusterTest {

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
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getResponse().getMatches(), arrayWithSize(2));
        assertThat(item.getResponse().getCount(), equalTo(2l));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("2", "4"));

        item =  response.getItems()[2];
        assertThat(item.errorMessage(), nullValue());
        assertThat(item.getResponse().getMatches(), arrayWithSize(4));
        assertThat(item.getResponse().getCount(), equalTo(4l));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContainingInAnyOrder("1", "2", "3", "4"));

        item = response.getItems()[3];
        assertThat(item.errorMessage(), nullValue());
        assertThat(item.getResponse().getMatches(), arrayWithSize(1));
        assertThat(item.getResponse().getCount(), equalTo(1l));
        assertThat(convertFromTextArray(item.getResponse().getMatches(), "test"), arrayContaining("4"));

        item = response.getItems()[4];
        assertThat(item.getResponse(), nullValue());
        assertThat(item.errorMessage(), notNullValue());
        assertThat(item.errorMessage(), containsString("document missing"));
    }

}
