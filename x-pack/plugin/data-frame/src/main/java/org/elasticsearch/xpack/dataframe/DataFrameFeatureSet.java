/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.dataframe.DataFrameFeatureSetUsage;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameTransformsStatsAction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

        GetDataFrameTransformsStatsAction.Request transformStatsRequest = new GetDataFrameTransformsStatsAction.Request(MetaData.ALL);

        client.execute(GetDataFrameTransformsStatsAction.INSTANCE, transformStatsRequest, ActionListener.wrap(transformStatsResponse -> {
            Map<String, Long> transformsCountByState = new HashMap<>();
            DataFrameIndexerTransformStats accumulatedStats = new DataFrameIndexerTransformStats();

            transformStatsResponse.getTransformsStateAndStats().stream().forEach(singleResult -> {
                transformsCountByState.merge(singleResult.getTransformState().getIndexerState().value(), 1L, Long::sum);
                accumulatedStats.merge(singleResult.getTransformStats());
            });

            listener.onResponse(new DataFrameFeatureSetUsage(available(), enabled(), transformsCountByState, accumulatedStats));
        }, listener::onFailure));
    }
}
