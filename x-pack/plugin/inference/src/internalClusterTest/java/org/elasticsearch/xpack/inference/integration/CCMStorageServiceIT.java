/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMIndex;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMStorageService;
import org.junit.Before;

import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CCMStorageServiceIT extends ESSingleNodeTestCase {
    private CCMStorageService ccmStorageService;

    @Before
    public void createComponents() {
        ccmStorageService = node().injector().getInstance(CCMStorageService.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateInferencePlugin.class);
    }

    public void testStoreAndGetCCMModel() {
        var ccmModel = new CCMModel(new SecureString("secret".toCharArray()));
        var storeListener = new PlainActionFuture<Void>();
        ccmStorageService.store(ccmModel, storeListener);

        assertNull(storeListener.actionGet(TimeValue.THIRTY_SECONDS));

        var getListener = new PlainActionFuture<CCMModel>();
        ccmStorageService.get(getListener);

        assertThat(getListener.actionGet(TimeValue.THIRTY_SECONDS), is(ccmModel));
    }

    public void testGet_ThrowsResourceNotFoundException_WhenCCMIndexDoesNotExist() {
        var getListener = new PlainActionFuture<CCMModel>();
        ccmStorageService.get(getListener);

        var exception = expectThrows(ResourceNotFoundException.class, () -> getListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), is("CCM configuration not found"));
    }

    public void testGet_ThrowsResourceNotFoundException_WhenCCMConfigurationDocumentDoesNotExist() {
        storeCorruptCCMModel("id");

        var getListener = new PlainActionFuture<CCMModel>();
        ccmStorageService.get(getListener);

        var exception = expectThrows(ResourceNotFoundException.class, () -> getListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), is("CCM configuration not found"));
    }

    public void testGetCCMModel_ThrowsException_WhenStoredModelIsCorrupted() {
        storeCorruptCCMModel(CCMStorageService.CCM_DOC_ID);

        var getListener = new PlainActionFuture<CCMModel>();
        ccmStorageService.get(getListener);

        var exception = expectThrows(ElasticsearchException.class, () -> getListener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), containsString("Failed to retrieve CCM configuration"));
        assertThat(exception.getCause().getMessage(), containsString("Required [api_key]"));
    }

    private void storeCorruptCCMModel(String id) {
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

}
