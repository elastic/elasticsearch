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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;

import java.io.IOException;
import java.util.Objects;

public class CCMPersistentStorageService {

    private static final Logger logger = LogManager.getLogger(CCMPersistentStorageService.class);
    public static final String CCM_DOC_ID = "ccm_config";

    private final Client client;

    public CCMPersistentStorageService(Client client) {
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
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
            logger.warn("Failed to persist CCM configuration", e);
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

        var searchListener = ActionListener.<SearchResponse>wrap(searchResponse -> {
            if (searchResponse.getHits().getHits().length == 0) {
                listener.onFailure(ccmConfigNotFound);
                return;
            }

            assert searchResponse.getHits().getHits().length == 1;
            listener.onResponse(CCMModel.fromXContentBytes(searchResponse.getHits().getHits()[0].getSourceRef()));
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onFailure(ccmConfigNotFound);
                return;
            }

            var message = "Failed to retrieve CCM configuration";
            logger.warn(message, e);
            listener.onFailure(new ElasticsearchException(message, e));
        });

        client.prepareSearch(CCMIndex.INDEX_PATTERN)
            .setSize(1)
            .setTrackTotalHits(false)
            .setQuery(QueryBuilders.idsQuery().addIds(CCM_DOC_ID))
            .execute(searchListener);
    }

    public void delete(ActionListener<Void> listener) {
        var request = new DeleteByQueryRequest().setAbortOnVersionConflict(false)
            .indices(CCMIndex.INDEX_PATTERN)
            .setRefresh(true)
            .setQuery(QueryBuilders.idsQuery().addIds(CCM_DOC_ID));

        client.execute(
            DeleteByQueryAction.INSTANCE,
            request,
            listener.delegateFailureIgnoreResponseAndWrap(delegate -> delegate.onResponse(null))
        );
    }
}
