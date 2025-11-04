/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.IOException;

public class CCMStorageService {

    private static final Logger logger = LogManager.getLogger(CCMStorageService.class);
    public static final String CCM_DOC_ID = "ccm_config";

    private final Client client;

    public CCMStorageService(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    public void store(CCMModel model, ActionListener<Void> listener) {
        try {
            client.prepareIndex()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setIndex(CCMIndex.INDEX_NAME)
                .setSource(createSource(model))
                .setId(CCM_DOC_ID)
                .execute(listener.delegateFailureIgnoreResponseAndWrap(delegate -> delegate.onResponse(null)));
        } catch (Exception e) {
            logger.warn("Failed to persist CCM configurations", e);
            listener.onFailure(e);
        }
    }

    private XContentBuilder createSource(CCMModel model) throws IOException {
        try (var xContentBuilder = XContentFactory.jsonBuilder()) {
            return model.toXContent(xContentBuilder, null);
        }
    }

    public void get(ActionListener<CCMModel> listener) {
        var ccmConfigNotFound = new ResourceNotFoundException("CCM configuration not found");

        var getResponseListener = ActionListener.<GetResponse>wrap(response -> {
            if (response.isExists() == false) {
                listener.onFailure(ccmConfigNotFound);
                return;
            }
            listener.onResponse(CCMModel.fromXContentBytes(response.getSourceAsBytesRef()));
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onFailure(ccmConfigNotFound);
                return;
            }

            var message = "Failed to retrieve CCM configuration";
            logger.warn(message, e);
            listener.onFailure(new ElasticsearchException(message, e));
        });

        client.prepareGet().setIndex(CCMIndex.INDEX_NAME).setId(CCM_DOC_ID).execute(getResponseListener);
    }
}
