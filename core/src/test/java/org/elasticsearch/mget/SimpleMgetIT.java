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
package org.elasticsearch.mget;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimpleMgetIT extends ESIntegTestCase {
    public void testThatMgetShouldWorkWithOneIndexMissing() throws IOException {
        createIndex("test");
        ensureYellow();

        client().prepareIndex("test", "test", "1").setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("test", "test", "1"))
                .add(new MultiGetRequest.Item("nonExistingIndex", "test", "1"))
                .execute().actionGet();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is("test"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), is("no such index"));
        assertThat(((ElasticsearchException) mgetResponse.getResponses()[1].getFailure().getFailure()).getIndex().getName(), is("nonExistingIndex"));


        mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item("nonExistingIndex", "test", "1"))
                .execute().actionGet();
        assertThat(mgetResponse.getResponses().length, is(1));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[0].getFailure().getMessage(), is("no such index"));
        assertThat(((ElasticsearchException) mgetResponse.getResponses()[0].getFailure().getFailure()).getIndex().getName(), is("nonExistingIndex"));


    }

    public void testThatParentPerDocumentIsSupported() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .addMapping("test", jsonBuilder()
                        .startObject()
                        .startObject("test")
                        .startObject("_parent")
                        .field("type", "foo")
                        .endObject()
                        .endObject()
                        .endObject()));
        ensureYellow();

        client().prepareIndex("test", "test", "1").setParent("4").setRefreshPolicy(IMMEDIATE)
                .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "test", "1").parent("4"))
                .add(new MultiGetRequest.Item(indexOrAlias(), "test", "1"))
                .execute().actionGet();

        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));

        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getResponse(), nullValue());
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), equalTo("routing is required for [test]/[test]/[1]"));
    }

    @SuppressWarnings("unchecked")
    public void testThatSourceFilteringIsSupported() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureYellow();
        BytesReference sourceBytesRef = jsonBuilder().startObject()
                .field("field", "1", "2")
                .startObject("included").field("field", "should be seen").field("hidden_field", "should not be seen").endObject()
                .field("excluded", "should not be seen")
                .endObject().bytes();
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource(sourceBytesRef).get();
        }

        MultiGetRequestBuilder request = client().prepareMultiGet();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                request.add(new MultiGetRequest.Item(indexOrAlias(), "type", Integer.toString(i)).fetchSourceContext(new FetchSourceContext("included", "*.hidden_field")));
            } else {
                request.add(new MultiGetRequest.Item(indexOrAlias(), "type", Integer.toString(i)).fetchSourceContext(new FetchSourceContext(false)));
            }
        }

        MultiGetResponse response = request.get();

        assertThat(response.getResponses().length, equalTo(100));
        for (int i = 0; i < 100; i++) {
            MultiGetItemResponse responseItem = response.getResponses()[i];
            assertThat(responseItem.getIndex(), equalTo("test"));
            if (i % 2 == 0) {
                Map<String, Object> source = responseItem.getResponse().getSourceAsMap();
                assertThat(source.size(), equalTo(1));
                assertThat(source, hasKey("included"));
                assertThat(((Map<String, Object>) source.get("included")).size(), equalTo(1));
                assertThat(((Map<String, Object>) source.get("included")), hasKey("field"));
            } else {
                assertThat(responseItem.getResponse().getSourceAsBytes(), nullValue());
            }
        }
    }

    public void testThatRoutingPerDocumentIsSupported() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(Settings.builder()
                        .put(indexSettings())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, between(2, DEFAULT_MAX_NUM_SHARDS))));
        ensureYellow();

        final String id = routingKeyForShard("test", 0);
        final String routingOtherShard = routingKeyForShard("test", 1);

        client().prepareIndex("test", "test", id).setRefreshPolicy(IMMEDIATE).setRouting(routingOtherShard)
                .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
                .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
                .add(new MultiGetRequest.Item(indexOrAlias(), "test", id).routing(routingOtherShard))
                .add(new MultiGetRequest.Item(indexOrAlias(), "test", id))
                .execute().actionGet();

        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(mgetResponse.getResponses()[0].getResponse().getIndex(), is("test"));

        assertThat(mgetResponse.getResponses()[1].isFailed(), is(false));
        assertThat(mgetResponse.getResponses()[1].getResponse().isExists(), is(false));
        assertThat(mgetResponse.getResponses()[1].getResponse().getIndex(), is("test"));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }
}
