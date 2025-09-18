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

    public void testSearchesAgainstNonMatchingIndices() throws Exception {
        testSearchesAgainstNonMatchingIndices(false);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("serverless.cross_project.enabled", "true").build();
    }
}
