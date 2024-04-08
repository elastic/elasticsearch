/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.mget;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SimpleMgetIT extends ESIntegTestCase {

    public void testThatMgetShouldWorkWithOneIndexMissing() throws IOException {
        createIndex("test");

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item("test", "1"))
            .add(new MultiGetRequest.Item("nonExistingIndex", "1"))
            .get();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is("test"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), is("no such index [nonExistingIndex]"));
        assertThat(
            ((ElasticsearchException) mgetResponse.getResponses()[1].getFailure().getFailure()).getIndex().getName(),
            is("nonExistingIndex")
        );

        mgetResponse = client().prepareMultiGet().add(new MultiGetRequest.Item("nonExistingIndex", "1")).get();
        assertThat(mgetResponse.getResponses().length, is(1));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is("nonExistingIndex"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[0].getFailure().getMessage(), is("no such index [nonExistingIndex]"));
        assertThat(
            ((ElasticsearchException) mgetResponse.getResponses()[0].getFailure().getFailure()).getIndex().getName(),
            is("nonExistingIndex")
        );
    }

    public void testThatMgetShouldWorkWithMultiIndexAlias() throws IOException {
        assertAcked(prepareCreate("test").addAlias(new Alias("multiIndexAlias")));
        assertAcked(prepareCreate("test2").addAlias(new Alias("multiIndexAlias")));

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item("test", "1"))
            .add(new MultiGetRequest.Item("multiIndexAlias", "1"))
            .get();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is("test"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(false));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("multiIndexAlias"));
        assertThat(mgetResponse.getResponses()[1].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[1].getFailure().getMessage(), containsString("more than one index"));

        mgetResponse = client().prepareMultiGet().add(new MultiGetRequest.Item("multiIndexAlias", "1")).get();
        assertThat(mgetResponse.getResponses().length, is(1));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is("multiIndexAlias"));
        assertThat(mgetResponse.getResponses()[0].isFailed(), is(true));
        assertThat(mgetResponse.getResponses()[0].getFailure().getMessage(), containsString("more than one index"));
    }

    public void testThatMgetShouldWorkWithAliasRouting() throws IOException {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias1").routing("abc"))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("_doc")
                        .startObject("_routing")
                        .field("required", true)
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        prepareIndex("alias1").setId("1")
            .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet().add(new MultiGetRequest.Item("alias1", "1")).get();
        assertEquals(1, mgetResponse.getResponses().length);

        assertEquals("test", mgetResponse.getResponses()[0].getIndex());
        assertFalse(mgetResponse.getResponses()[0].isFailed());
    }

    @SuppressWarnings("unchecked")
    public void testThatSourceFilteringIsSupported() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        BytesReference sourceBytesRef = BytesReference.bytes(
            jsonBuilder().startObject()
                .array("field", "1", "2")
                .startObject("included")
                .field("field", "should be seen")
                .field("hidden_field", "should not be seen")
                .endObject()
                .field("excluded", "should not be seen")
                .endObject()
        );
        for (int i = 0; i < 100; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource(sourceBytesRef, XContentType.JSON).get();
        }

        MultiGetRequestBuilder request = client().prepareMultiGet();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                request.add(
                    new MultiGetRequest.Item(indexOrAlias(), Integer.toString(i)).fetchSourceContext(
                        FetchSourceContext.of(true, new String[] { "included" }, new String[] { "*.hidden_field" })
                    )
                );
            } else {
                request.add(
                    new MultiGetRequest.Item(indexOrAlias(), Integer.toString(i)).fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
                );
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
                assertThat(responseItem.getResponse().getSourceAsBytesRef(), nullValue());
            }
        }
    }

    public void testThatRoutingPerDocumentIsSupported() throws Exception {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(
                    Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(2, DEFAULT_MAX_NUM_SHARDS))
                )
        );

        final String id = routingKeyForShard("test", 0);
        final String routingOtherShard = routingKeyForShard("test", 1);

        prepareIndex("test").setId(id)
            .setRefreshPolicy(IMMEDIATE)
            .setRouting(routingOtherShard)
            .setSource(jsonBuilder().startObject().field("foo", "bar").endObject())
            .get();

        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), id).routing(routingOtherShard))
            .add(new MultiGetRequest.Item(indexOrAlias(), id))
            .get();

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
