/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.xpack.core.inference.action.GetInferenceFieldsAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class GetInferenceFieldsCrossClusterIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER = "cluster_a";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, DEFAULT_SKIP_UNAVAILABLE);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(LocalStateInferencePlugin.class);
    }

    public void testRemoteIndex() {
        var request = new GetInferenceFieldsAction.Request(Set.of(REMOTE_CLUSTER + ":test-index"), Set.of(), false, false, "foo");
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> client().execute(GetInferenceFieldsAction.INSTANCE, request).actionGet(TEST_REQUEST_TIMEOUT)
        );
        assertThat(e.getMessage(), containsString("GetInferenceFieldsAction does not support remote indices"));
    }
}
