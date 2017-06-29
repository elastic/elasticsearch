/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction;
import org.elasticsearch.xpack.upgrade.actions.IndexUpgradeInfoAction.Response;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

public class IndexUpgradeIT extends IndexUpgradeIntegTestCase {

    @Before
    public void resetLicensing() throws Exception {
        enableLicensing();
    }

    public void testIndexUpgradeInfo() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        assertAcked(client().admin().indices().prepareCreate("kibana_test").get());
        ensureYellow("test", "kibana_test");
        Response response = client().prepareExecute(IndexUpgradeInfoAction.INSTANCE).setIndices("test", "kibana_test")
                .setExtraParams(Collections.singletonMap("kibana_indices", "kibana_test")).get();
        logger.info("Got response [{}]", Strings.toString(response));
        assertThat(response.getActions().size(), equalTo(1));
        assertThat(response.getActions().get("kibana_test"), equalTo(UpgradeActionRequired.UPGRADE));
        assertThat(Strings.toString(response), containsString("kibana_test"));
    }

    public void testIndexUpgradeInfoLicense() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        ensureYellow("test");
        disableLicensing();
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> client().prepareExecute(IndexUpgradeInfoAction.INSTANCE).setIndices("test").get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [upgrade]"));
        enableLicensing();
        Response response = client().prepareExecute(IndexUpgradeInfoAction.INSTANCE).setIndices("test").get();
        assertThat(response.getActions().entrySet(), empty());
    }

    public void testUpgradeInternalIndex() throws Exception {
        String testIndex = ".kibana";
        String testType = "doc";
        assertAcked(client().admin().indices().prepareCreate(testIndex).get());
        indexRandom(true,
                client().prepareIndex(testIndex, testType, "1").setSource("{\"foo\":\"bar\"}", XContentType.JSON),
                client().prepareIndex(testIndex, testType, "2").setSource("{\"foo\":\"baz\"}", XContentType.JSON)
        );
        ensureYellow(testIndex);

        BulkByScrollResponse response = client().prepareExecute(IndexUpgradeAction.INSTANCE).setIndex(testIndex).get();
        assertThat(response.getCreated(), equalTo(2L));

        SearchResponse searchResponse = client().prepareSearch(testIndex).get();
        assertEquals(2L, searchResponse.getHits().getTotalHits());
    }

    public void testInternalUpgradePrePostChecks() {
        Long val = randomLong();
        AtomicBoolean preUpgradeIsCalled = new AtomicBoolean();
        AtomicBoolean postUpgradeIsCalled = new AtomicBoolean();

        IndexUpgradeCheck check = new IndexUpgradeCheck<Long>(
                "test", Settings.EMPTY,
                (indexMetaData, stringStringMap) -> {
                    if (indexMetaData.getIndex().getName().equals("internal_index")) {
                        return UpgradeActionRequired.UPGRADE;
                    } else {
                        return UpgradeActionRequired.NOT_APPLICABLE;
                    }
                },
                client(), internalCluster().clusterService(internalCluster().getMasterName()), Strings.EMPTY_ARRAY, null,
                listener -> {
                    assertFalse(preUpgradeIsCalled.getAndSet(true));
                    assertFalse(postUpgradeIsCalled.get());
                    listener.onResponse(val);
                },
                (aLong, listener) -> {
                    assertTrue(preUpgradeIsCalled.get());
                    assertFalse(postUpgradeIsCalled.getAndSet(true));
                    assertEquals(aLong, val);
                    listener.onResponse(TransportResponse.Empty.INSTANCE);
                });

        assertAcked(client().admin().indices().prepareCreate("internal_index").get());

        IndexUpgradeService service = new IndexUpgradeService(Settings.EMPTY, Collections.singletonList(check));

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        service.upgrade("internal_index", Collections.emptyMap(), clusterService().state(), future);
        future.actionGet();

        assertTrue(preUpgradeIsCalled.get());
        assertTrue(postUpgradeIsCalled.get());
    }

}
