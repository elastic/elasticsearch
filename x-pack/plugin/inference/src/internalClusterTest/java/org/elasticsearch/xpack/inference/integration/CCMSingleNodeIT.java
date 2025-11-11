/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMIndex;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;

import java.util.Collection;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMPersistentStorageService.CCM_DOC_ID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public abstract class CCMSingleNodeIT extends ESSingleNodeTestCase {

    private final Provider provider;

    public interface Provider {
        void store(CCMModel ccmModel, ActionListener<Void> listener);

        void get(ActionListener<CCMModel> listener);

        void delete(ActionListener<Void> listener);
    }

    public CCMSingleNodeIT(Provider provider) {
        this.provider = Objects.requireNonNull(provider);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testStoreAndGetCCMModel() {
        assertStoreCCMConfiguration();
    }

    protected void assertStoreCCMConfiguration() {
        assertStoreCCMConfiguration("secret");
    }

    protected void assertStoreCCMConfiguration(String apiKey) {
        var ccmModel = new CCMModel(new SecureString(apiKey.toCharArray()));
        var storeListener = new PlainActionFuture<Void>();
        provider.store(ccmModel, storeListener);

        assertNull(storeListener.actionGet(TimeValue.THIRTY_SECONDS));

        var getListener = new PlainActionFuture<CCMModel>();
        provider.get(getListener);

        assertThat(getListener.actionGet(TimeValue.THIRTY_SECONDS), is(ccmModel));
    }

    public void testStore_OverwritesConfiguration_WhenItAlreadyExists() {
        assertStoreCCMConfiguration();
        assertStoreCCMConfiguration("new_secret");

        assertHitCount(client().prepareSearch(CCMIndex.INDEX_PATTERN).setQuery(QueryBuilders.idsQuery().addIds(CCM_DOC_ID)), 1);
    }

    public void testGet_ThrowsResourceNotFoundException_WhenCCMIndexDoesNotExist() {
        assertCCMResourceDoesNotExist();
    }

    protected void assertCCMResourceDoesNotExist() {
        var getListener = new PlainActionFuture<CCMModel>();
        provider.get(getListener);

        var exception = expectThrows(ResourceNotFoundException.class, () -> getListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), is("CCM configuration not found"));
    }

    public void testGet_ThrowsResourceNotFoundException_WhenCCMConfigurationDocumentDoesNotExist() {
        storeCorruptCCMModel("id");

        assertCCMResourceDoesNotExist();
    }

    public void testGetCCMModel_ThrowsException_WhenStoredModelIsCorrupted() {
        storeCorruptCCMModel(CCM_DOC_ID);

        var getListener = new PlainActionFuture<CCMModel>();
        provider.get(getListener);

        var exception = expectThrows(ElasticsearchException.class, () -> getListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), containsString("Failed to retrieve CCM configuration"));
        assertThat(exception.getCause().getMessage(), containsString("Required [api_key]"));
    }

    protected void storeCorruptCCMModel(String id) {
        var corruptedSource = """
            {

            }
            """;

        var response = client().prepareIndex()
            .setSource(corruptedSource, XContentType.JSON)
            .setIndex(CCMIndex.INDEX_NAME)
            .setId(id)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet(TimeValue.THIRTY_SECONDS);

        assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
    }

    public void testDelete_DoesNotThrow_WhenTheConfigurationDoesNotExist() {
        var listener = new PlainActionFuture<Void>();
        provider.delete(listener);

        assertNull(listener.actionGet(TimeValue.THIRTY_SECONDS));
        assertCCMResourceDoesNotExist();
    }

    public void testDelete_RemovesCCMConfiguration() {
        assertStoreCCMConfiguration();

        var listener = new PlainActionFuture<Void>();
        provider.delete(listener);

        assertNull(listener.actionGet(TimeValue.THIRTY_SECONDS));
        assertCCMResourceDoesNotExist();
    }
}
