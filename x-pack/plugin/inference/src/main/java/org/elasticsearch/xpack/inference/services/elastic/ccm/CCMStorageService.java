/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.IOException;

public class CCMStorageService {

    private static final Logger logger = LogManager.getLogger(CCMStorageService.class);
    private static final String CCM_DOC_ID = "ccm_config";

    private final Client client;

    public CCMStorageService(Client client) {
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    public void store(CCMModel model) {
        try (var xContentBuilder = XContentFactory.jsonBuilder()) {
            var source = model.toXContent(xContentBuilder, null);
            var builder = client.prepareIndex()
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setIndex(CCMIndex.INDEX_NAME)
                .setSource(source).setId(CCM_DOC_ID).execute();

        } catch (IOException e) {

        }

    }
}
