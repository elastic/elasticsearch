/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.dataframe.transforms.DataFrameIndexer.COMPOSITE_AGGREGATION_NAME;

/**
 * Class to enable previewing the documents that would result from the provided config
 */
public class DataFramePreviewer {

    private final Map<String, String> headers;
    private final Pivot pivot;

    public DataFramePreviewer(DataFrameTransformConfig config,
                              Map<String, String> headers) {
        this.headers = headers;
        this.pivot = new Pivot(config.getSource(), config.getQueryConfig().getQuery(), config.getPivotConfig());
    }

    public void getPreview(Client client, ActionListener<List<Map<String, Object>>> listener) {
        ClientHelper.executeWithHeadersAsync(headers,
            ClientHelper.DATA_FRAME_ORIGIN,
            client,
            SearchAction.INSTANCE,
            pivot.buildSearchRequest(null),
            ActionListener.wrap(
                r -> {
                    final CompositeAggregation agg = r.getAggregations().get(COMPOSITE_AGGREGATION_NAME);
                    DataFrameIndexerTransformStats stats = new DataFrameIndexerTransformStats();
                    listener.onResponse(pivot.extractResults(agg, stats).collect(Collectors.toList()));
                },
                listener::onFailure
            ));
    }
}
