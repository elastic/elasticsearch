/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.mget;

import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;

public class SimpleMgetTests extends ElasticsearchIntegrationTest {

    @Test
    public void testThatMgetShouldWorkWithOneIndexMissing() throws IOException {
        createIndex("test");
        ensureYellow();

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject().field("foo", "bar").endObject()).setRefresh(true).execute().actionGet();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "test", "1"))
                .add(new MultiGetRequest.Item("nonExistingIndex", "test", "1"))
                .execute().actionGet();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is("test"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), is("[nonExistingIndex] missing"));


        mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("nonExistingIndex", "test", "1"))
                .execute().actionGet();
        assertThat(mgetResponse.getResponses().length, is(1));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[0].getFailure().getMessage(), is("[nonExistingIndex] missing"));

    }

    @Test
    public void testThatParentPerDocumentIsSupported() throws Exception {
        createIndex("test");
        ensureYellow();
        client().admin().indices().preparePutMapping("test").setType("test").setSource(jsonBuilder()
                .startObject()
                .startObject("test")
                .startObject("_parent")
                .field("type", "foo")
                .endObject()
                .endObject().
                        endObject()
        ).execute().actionGet();

        client().prepareIndex("test", "test", "1").setParent("4").setRefresh(true)
                .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .execute().actionGet();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "test", "1").parent("4"))
                .add(new MultiGetRequest.Item("test", "test", "1"))
                .execute().actionGet();

        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));

        assertThat(mgetResponse.getResponses()[1].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(false));
    }

    @Test
    public void testThatRoutingPerDocumentIsSupported() throws Exception {
        createIndex("test");
        ensureYellow();

        client().prepareIndex("test", "test", "1").setRefresh(true).setRouting("bar")
                .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .execute().actionGet();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "test", "1").routing("bar"))
                .add(new MultiGetRequest.Item("test", "test", "1"))
                .execute().actionGet();

        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));

        assertThat(mgetResponse.getResponses()[1].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(false));
    }
}
