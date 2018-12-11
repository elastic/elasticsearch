/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoResponse;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.IsEqual.equalTo;

public class IndexUpgradeIT extends IndexUpgradeIntegTestCase {

    @Before
    public void resetLicensing() throws Exception {
        enableLicensing();
    }

    public void testIndexUpgradeInfo() {
        // Testing only negative case here, the positive test is done in bwcTests
        assertAcked(client().admin().indices().prepareCreate("test").get());
        ensureYellow("test");
        IndexUpgradeInfoResponse response = new IndexUpgradeInfoAction.RequestBuilder(client()).setIndices("test").get();
        assertThat(response.getActions().entrySet(), empty());
    }

    public void testIndexUpgradeInfoLicense() throws Exception {
        // This test disables all licenses and generates a new one using dev private key
        // in non-snapshot builds we are using production public key for license verification
        // which makes this test to fail
        assumeTrue("License is only valid when tested against snapshot/test keys", Build.CURRENT.isSnapshot());
        assertAcked(client().admin().indices().prepareCreate("test").get());
        ensureYellow("test");
        disableLicensing();
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> new IndexUpgradeInfoAction.RequestBuilder(client()).setIndices("test").get());
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [upgrade]"));
        enableLicensing();
        IndexUpgradeInfoResponse response = new IndexUpgradeInfoAction.RequestBuilder(client()).setIndices("test").get();
        assertThat(response.getActions().entrySet(), empty());
    }

    public void testUpToDateIndexUpgrade() throws Exception {
        // Testing only negative case here, the positive test is done in bwcTests
        String testIndex = "test";
        String testType = "doc";
        assertAcked(client().admin().indices().prepareCreate(testIndex).get());
        indexRandom(true,
                client().prepareIndex(testIndex, testType, "1").setSource("{\"foo\":\"bar\"}", XContentType.JSON),
                client().prepareIndex(testIndex, testType, "2").setSource("{\"foo\":\"baz\"}", XContentType.JSON)
        );
        ensureYellow(testIndex);

        IllegalStateException ex = expectThrows(IllegalStateException.class,
                () -> new IndexUpgradeAction.RequestBuilder(client()).setIndex(testIndex).get());
        assertThat(ex.getMessage(), equalTo("Index [" + testIndex + "] cannot be upgraded"));

        SearchResponse searchResponse = client().prepareSearch(testIndex).get();
        assertEquals(2L, searchResponse.getHits().getTotalHits().value);
    }

    public void testInternalUpgradePrePostChecks() throws Exception {
        String testIndex = "internal_index";
        String testType = "test";
        Long val = randomLong();
        AtomicBoolean preUpgradeIsCalled = new AtomicBoolean();
        AtomicBoolean postUpgradeIsCalled = new AtomicBoolean();

        IndexUpgradeCheck check = new IndexUpgradeCheck<Long>(
                "test",
                indexMetaData -> {
                    if (indexMetaData.getIndex().getName().equals(testIndex)) {
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

        assertAcked(client().admin().indices().prepareCreate(testIndex).get());
        indexRandom(true,
                client().prepareIndex(testIndex, testType, "1").setSource("{\"foo\":\"bar\"}", XContentType.JSON),
                client().prepareIndex(testIndex, testType, "2").setSource("{\"foo\":\"baz\"}", XContentType.JSON)
        );
        ensureYellow(testIndex);

        IndexUpgradeService service = new IndexUpgradeService(Collections.singletonList(check));

        PlainActionFuture<BulkByScrollResponse> future = PlainActionFuture.newFuture();
        service.upgrade(new TaskId("abc", 123), testIndex, clusterService().state(), future);
        BulkByScrollResponse response = future.actionGet();
        assertThat(response.getCreated(), equalTo(2L));

        SearchResponse searchResponse = client().prepareSearch(testIndex).get();
        assertEquals(2L, searchResponse.getHits().getTotalHits().value);

        assertTrue(preUpgradeIsCalled.get());
        assertTrue(postUpgradeIsCalled.get());
    }

    public void testIndexUpgradeInfoOnEmptyCluster() {
        // On empty cluster asking for all indices shouldn't fail since no indices means nothing needs to be upgraded
        IndexUpgradeInfoResponse response = new IndexUpgradeInfoAction.RequestBuilder(client()).setIndices("_all").get();
        assertThat(response.getActions().entrySet(), empty());

        // but calling on a particular index should fail
        assertThrows(new IndexUpgradeInfoAction.RequestBuilder(client()).setIndices("test"), IndexNotFoundException.class);
    }
}
