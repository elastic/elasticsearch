/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

public class CrossProjectQueryIT extends CrossClusterQueryIT {
    public static class CpsPlugin extends Plugin implements ClusterPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(CpsEnableSetting);
        }
    }

    private static final Setting<String> CpsEnableSetting = Setting.simpleString(
        "serverless.cross_project.enabled",
        Setting.Property.NodeScope
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), CpsPlugin.class);
    }

    protected EsqlQueryResponse runQuery(String query, Boolean cpsMetadataInResponse) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (cpsMetadataInResponse != null) {
            if (randomBoolean()) {
                // sometimes set include_ccs_metadata, it will be overridden by include_cps_metadata
                request.includeCCSMetadata(randomBoolean());
            }
            request.includeCPSMetadata(cpsMetadataInResponse);
        }
        return runQuery(request);
    }

    public void testSearchesAgainstNonMatchingIndices() throws Exception {
        testSearchesAgainstNonMatchingIndices(false);
    }

    @Override
    public void testNoCps() {
        // do nothing, we have cps enabled
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }
}
