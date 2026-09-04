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
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", true).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(CpsPlugin.class)).toList();
    }

    public void testCrossProjectFailsForDataFrameAnalytics() throws IOException {
        var id = "test-cross-project-fails";
        var sourceIndex = "project1:" + id + "_source_index";
        var destIndex = id + "_results";

        var request = buildRequest(id, new String[] { sourceIndex }, destIndex);
        var response = client().execute(PutDataFrameAnalyticsAction.INSTANCE, request);
        var validationException = assertThrows(ValidationException.class, response::actionGet);
        assertThat(validationException.getMessage(), containsString("remote source and cross-project indices are not supported"));
    }

    public void testDFAAllowsUnqualifiedSource() throws IOException {
        var id = "test-allows-unqualified";
        // unqualified patterns are local-only in CPS (cross-project expansion is opt-in via explicit qualifier);
        // no ValidationException expected
        var request = buildRequest(id, new String[] { "logs-*" }, id + "_results");
        client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    public void testDFAAllowsOriginQualifier() throws IOException {
        var id = "test-allows-origin-qualifier";
        // _origin: is the CPS local-project qualifier and must be accepted (not treated as a remote cluster)
        var request = buildRequest(id, new String[] { "_origin:logs-*" }, id + "_results");
        client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();
    }

    public void testDFARejectsMixedLocalAndCrossProjectIndices() throws IOException {
        var id = "test-rejects-mixed";
        var request = buildRequest(id, new String[] { "logs-*", "project1:remote-logs-*" }, id + "_results");
        var response = client().execute(PutDataFrameAnalyticsAction.INSTANCE, request);
        var validationException = assertThrows(ValidationException.class, response::actionGet);
        assertThat(validationException.getMessage(), containsString("remote source and cross-project indices are not supported"));
    }

    private static PutDataFrameAnalyticsAction.Request buildRequest(String id, String[] sourceIndices, String destIndex)
        throws IOException {
        return new PutDataFrameAnalyticsAction.Request(
            new DataFrameAnalyticsConfig.Builder().setId(id)
                .setSource(
                    new DataFrameAnalyticsSource(
                        sourceIndices,
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
                .build()
        );
    }

    public static class CpsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }
}
