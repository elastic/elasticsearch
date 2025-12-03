/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class CCMCacheTests extends ESSingleNodeTestCase {

    private static final TimeValue TIMEOUT = TimeValue.THIRTY_SECONDS;

    private CCMCache ccmCache;
    private CCMPersistentStorageService ccmPersistentStorageService;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Before
    public void createComponents() {
        ccmCache = node().injector().getInstance(CCMCache.class);
        ccmPersistentStorageService = node().injector().getInstance(CCMPersistentStorageService.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @After
    public void clearCacheAndIndex() {
        try {
            indicesAdmin().prepareDelete(CCMIndex.INDEX_NAME).execute().actionGet(TIMEOUT);
        } catch (ResourceNotFoundException e) {
            // mission complete!
        }
    }

    public void testCacheHit() throws IOException {
        var expectedCcmModel = storeCcm();
        var actualCcmModel = getFromCache();
        assertThat(actualCcmModel, equalTo(expectedCcmModel));
        assertThat(ccmCache.stats().getHits(), equalTo(0L));
        assertThat(getFromCache(), sameInstance(actualCcmModel));
        assertThat(ccmCache.stats().getHits(), equalTo(1L));
    }

    private CCMModel storeCcm() throws IOException {
        var ccmModel = CCMModel.fromXContentBytes(new BytesArray("""
            {
                "api_key": "test_key"
            }
            """));
        var listener = new TestPlainActionFuture<Void>();
        ccmPersistentStorageService.store(ccmModel, listener);
        listener.actionGet(TIMEOUT);
        return ccmModel;
    }

    private CCMModel getFromCache() {
        var listener = new TestPlainActionFuture<CCMModel>();
        ccmCache.get(listener);
        return listener.actionGet(TIMEOUT);
    }

    public void testCacheInvalidate() throws Exception {
        var expectedCcmModel = storeCcm();
        var actualCcmModel = getFromCache();
        assertThat(actualCcmModel, equalTo(expectedCcmModel));
        assertThat(ccmCache.stats().getHits(), equalTo(0L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));
        assertThat(ccmCache.cacheCount(), equalTo(1));

        var listener = new TestPlainActionFuture<Void>();
        ccmCache.invalidate(listener);
        listener.actionGet(TIMEOUT);

        assertThat(getFromCache(), not(sameInstance(actualCcmModel)));
        assertThat(ccmCache.stats().getHits(), equalTo(0L));
        assertThat(ccmCache.stats().getMisses(), equalTo(2L));
        assertThat(ccmCache.stats().getEvictions(), equalTo(1L));
        assertThat(ccmCache.cacheCount(), equalTo(1));
    }

    public void testEmptyInvalidate() throws InterruptedException {
        var latch = new CountDownLatch(1);
        ccmCache.invalidate(ActionTestUtils.assertNoFailureListener(success -> latch.countDown()));
        assertTrue(latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));

        assertThat(ccmCache.stats().getEvictions(), equalTo(0L));
        assertThat(ccmCache.cacheCount(), equalTo(0));
    }

    private boolean isPresent() {
        var listener = new TestPlainActionFuture<Boolean>();
        ccmCache.isEnabled(listener);
        return listener.actionGet(TIMEOUT);
    }

    public void testIsEnabled() throws IOException {
        storeCcm();

        getFromCache();
        assertThat(ccmCache.stats().getHits(), equalTo(0L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));

        assertTrue(isPresent());
        assertThat(ccmCache.stats().getHits(), equalTo(1L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));
    }

    public void testIsDisabledWithMissingIndex() {
        assertFalse(isPresent());
    }

    public void testIsDisabledWithPresentIndex() {
        indicesAdmin().prepareCreate(CCMIndex.INDEX_NAME).execute().actionGet(TIMEOUT);
        assertFalse(isPresent());
    }

    public void testIsDisabledWithCacheHit() {
        indicesAdmin().prepareCreate(CCMIndex.INDEX_NAME).execute().actionGet(TIMEOUT);

        assertFalse(isPresent());
        assertThat(ccmCache.stats().getHits(), equalTo(0L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));

        assertFalse(isPresent());
        assertThat(ccmCache.stats().getHits(), equalTo(1L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));
    }

    public void testIsDisabledRefreshedWithGet() throws IOException {
        indicesAdmin().prepareCreate(CCMIndex.INDEX_NAME).execute().actionGet(TIMEOUT);

        assertFalse(isPresent());
        assertThat(ccmCache.stats().getHits(), equalTo(0L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));

        var expectedCcmModel = storeCcm();

        assertFalse(isPresent());
        assertThat(ccmCache.stats().getHits(), equalTo(1L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));

        var actualCcmModel = getFromCache();
        assertThat(actualCcmModel, equalTo(expectedCcmModel));
        assertThat(ccmCache.stats().getHits(), equalTo(2L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));

        assertTrue(isPresent());
        assertThat(ccmCache.stats().getHits(), equalTo(3L));
        assertThat(ccmCache.stats().getMisses(), equalTo(1L));
    }
}
