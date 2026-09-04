/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

/**
 * The PR's headline promise, end to end: on a cluster without the licence for approximation, an operator's
 * cluster-wide default must not fail queries from users who never asked for it — while a user who asks explicitly
 * still gets the licensing error. Unit coverage pins the decision; this pins that the decision survives a real query.
 */
public class ApproximationClusterDefaultWithInvalidLicenseIT extends AbstractEsqlIntegTestCase {

    private static final String APPROXIMATION_KEY = QuerySettingDef.CLUSTER_SETTING_PREFIX + QuerySettings.APPROXIMATION.name();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestEncryptionServicePlugin.class, EsqlPluginWithNonEnterpriseOrExpiredLicense.class);
    }

    @After
    public void clearClusterDefaults() {
        updateClusterSettings(Settings.builder().putNull(APPROXIMATION_KEY));
    }

    public void testOperatorDefaultDoesNotFailQueriesOnAnUnlicensedCluster() {
        updateClusterSettings(Settings.builder().put(APPROXIMATION_KEY, "true"));

        // The query runs. Failing here would break every query on the cluster for people who did not set this and
        // cannot turn it off, which is the whole reason the drop is silent rather than an error.
        try (EsqlQueryResponse response = run("ROW x = 1")) {
            // And it ran exactly: the marker is null, so the default did not apply.
            assertThat(response.approximationApplied(), nullValue());
        }
    }

    public void testUserAskingExplicitlyStillGetsTheLicenseError() {
        // Provenance, not the value, decides. The same value a user supplies is refused.
        updateClusterSettings(Settings.builder().put(APPROXIMATION_KEY, "true"));
        var e = expectThrows(Exception.class, () -> run("SET approximation = true; ROW x = 1").close());
        assertThat(e.getMessage(), containsString("license"));
    }
}
