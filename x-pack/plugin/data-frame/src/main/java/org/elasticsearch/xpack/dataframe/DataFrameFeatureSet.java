/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.dataframe.DataFrameFeatureSetUsage;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameInternalIndex;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.DATA_FRAME_ORIGIN;

public class DataFrameFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final Client client;
    private final XPackLicenseState licenseState;

    @Inject
    public DataFrameFeatureSet(Settings settings, Client client, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.DATA_FRAME_ENABLED.get(settings);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.DATA_FRAME;
    }

    @Override
    public String description() {
        return "Data Frame for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isDataFrameAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        if (enabled == false) {
            listener.onResponse(
                    new DataFrameFeatureSetUsage(available(), enabled(), Collections.emptyMap(), new DataFrameIndexerTransformStats()));
            return;
        }

        final GetDataFrameTransformsStatsAction.Request transformStatsRequest = new GetDataFrameTransformsStatsAction.Request(MetaData.ALL);
        ActionListener<SearchResponse> configHitListener = ActionListener.wrap(
            countResponse -> {
                final long totalConfigs = countResponse.getHits().getTotalHits().value;
                client.execute(GetDataFrameTransformsStatsAction.INSTANCE,
                    transformStatsRequest,
                    ActionListener.wrap(transformStatsResponse -> {
                        listener.onResponse(createUsage(available(),
                            enabled(),
                            totalConfigs,
                            transformStatsResponse.getTransformsStateAndStats()));
                    }, listener::onFailure));
            },
            exception -> {
                // We should create an empty but enabled response if we have not created the transforms index yet.
                if (exception instanceof IndexNotFoundException) {
                    listener.onResponse(new DataFrameFeatureSetUsage(available(),
                        enabled(),
                        new HashMap<>(),
                        new DataFrameIndexerTransformStats()));
                } else {
                    listener.onFailure(exception);
                }
            }
        );

        SearchRequest searchRequest = client.prepareSearch(DataFrameInternalIndex.INDEX_NAME)
            .setSize(0) // We only care about the hit count
            .setTrackTotalHits(true) // Need all the hits to get an accurate count of configs
            .setQuery(QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), DataFrameTransformConfig.NAME)))
            .request();

        ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(),
            DATA_FRAME_ORIGIN,
            searchRequest,
            configHitListener,
            client::search);
    }

    static DataFrameFeatureSetUsage createUsage(boolean available,
                                                boolean enabled,
                                                long numberOfTransforms,
                                                List<DataFrameTransformStateAndStats> transformsStateAndStats) {

        Map<String, Long> transformsCountByState = new HashMap<>();
        DataFrameIndexerTransformStats accumulatedStats = new DataFrameIndexerTransformStats();
        transformsStateAndStats.forEach(singleResult -> {
            transformsCountByState.merge(singleResult.getTransformState().getIndexerState().value(), 1L, Long::sum);
            accumulatedStats.merge(singleResult.getTransformStats());
        });

        // How many configs do we have that do not have a task?
        // This implies that the same number of configs have a state of STOPPED
        long configsWithOutTask = numberOfTransforms - transformsStateAndStats.size();
        transformsCountByState.merge(IndexerState.STOPPED.value(), configsWithOutTask, Long::sum);

        return new DataFrameFeatureSetUsage(available, enabled, transformsCountByState, accumulatedStats);
    }
}
