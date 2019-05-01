/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;

/**
 * Utility class to gather the progress information for a given config and its cursor position
 */
public final class TransformProgressGatherer {

    /**
     * This gathers the total docs given the config and search
     *
     * TODO: Support checkpointing logic to restrict the query
     * @param progressListener The listener to alert on completion
     */
    public static void getInitialProgress(Client client,
                                          DataFrameTransformConfig config,
                                          ActionListener<DataFrameTransformProgress> progressListener) {
        SearchRequest request = client.prepareSearch(config.getSource().getIndex())
            .setSize(0)
            .setAllowPartialSearchResults(false)
            .setTrackTotalHits(true)
            .setQuery(config.getSource().getQueryConfig().getQuery())
            .request();

        ActionListener<SearchResponse> searchResponseActionListener = ActionListener.wrap(
            searchResponse -> {
                progressListener.onResponse(new DataFrameTransformProgress(searchResponse.getHits().getTotalHits().value, null));
            },
            progressListener::onFailure
        );
        ClientHelper.executeWithHeadersAsync(config.getHeaders(),
            ClientHelper.DATA_FRAME_ORIGIN,
            client,
            SearchAction.INSTANCE,
            request,
            searchResponseActionListener);
    }

}
