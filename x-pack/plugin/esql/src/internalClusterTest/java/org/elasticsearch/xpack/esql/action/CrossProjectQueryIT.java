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
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

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

    protected EsqlQueryResponse runQuery(String query, Boolean includeCpsMetadata) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (includeCpsMetadata != null) {
            if (randomBoolean()) {
                request.includeCPSMetadata(includeCpsMetadata);
            } else {
                request.includeCCSMetadata(includeCpsMetadata);
            }
        }
        return runQuery(request);
    }

    public void testSearchesAgainstNonMatchingIndices() throws Exception {
        testSearchesAgainstNonMatchingIndices(false);
    }

    public void testNoBothCcsAndCpsFlags() throws Exception {
        setupTwoClusters();
        var query = "from logs-*,c*:logs-* | stats sum (v)";
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        request.includeCCSMetadata(randomBoolean());
        request.includeCPSMetadata(randomBoolean());

        assertThat(
            expectThrows(VerificationException.class, () -> runQuery(request)).getMessage(),
            containsString(
                "Both [include_cps_metadata] and [include_ccs_metadata] query parameters are set. Use only [include_cps_metadata]"
            )
        );
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
