/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;

public class DataframeCpsIT extends MlSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(CpsPlugin.class)).toList();
    }

    public void testCrossProjectFailsForDataFrameAnalytics() throws IOException {
        var id = "test-cross-project-fails";
        var sourceIndex = "project1:" + id + "_source_index";
        var destIndex = id + "_results";

        var config = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setSource(
                new DataFrameAnalyticsSource(
                    new String[] { sourceIndex },
                    QueryProvider.fromParsedQuery(QueryBuilders.matchAllQuery()),
                    null,
                    Collections.emptyMap()
                )
            )
            .setDest(new DataFrameAnalyticsDest(destIndex, null))
            .setAnalysis(
                new Classification(
                    "keyword-field",
                    BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .build();

        var request = new PutDataFrameAnalyticsAction.Request(config);
        var response = client().execute(PutDataFrameAnalyticsAction.INSTANCE, request);
        var validationException = assertThrows(ValidationException.class, response::actionGet);
        assertThat(validationException.getMessage(), containsString("remote source and cross-project indices are not supported"));
    }

    public static class CpsPlugin extends Plugin implements ClusterPlugin {
        public List<Setting<?>> getSettings() {
            return List.of(Setting.simpleString("serverless.cross_project.enabled", Setting.Property.NodeScope));
        }
    }
}
