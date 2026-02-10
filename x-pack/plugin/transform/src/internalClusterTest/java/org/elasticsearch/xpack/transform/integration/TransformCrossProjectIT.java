/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class TransformCrossProjectIT extends TransformSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Stream.concat(super.getPlugins().stream(), Stream.of(CpsPlugin.class)).toList();
    }

    public static class CpsPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(Setting.boolSetting("serverless.cross_project.enabled", false, Setting.Property.NodeScope));
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .put("serverless.cross_project.enabled", true)
            .build();
    }

    public void testGetTransformWithProjectRouting() {
        var transformSrc = "reviews_transform_project_routing";
        createSourceIndex(transformSrc);
        indexRandomDiceDoc(transformSrc);

        var transformId = "transform_project_routing";

        var expectedProjectRouting = "_alias:_origin";

        createTransform(
            TransformConfig.builder()
                .setId(transformId)
                .setDest(new DestConfig(transformId, null, null))
                .setSource(new SourceConfig(new String[] { transformSrc }, QueryConfig.matchAll(), Map.of(), expectedProjectRouting))
                .setFrequency(TimeValue.ONE_MINUTE)
                .setSyncConfig(new TimeSyncConfig("time", TimeValue.ONE_MINUTE))
                .setLatestConfig(new LatestConfig(List.of("roll"), "time"))
                .build()
        );

        var transformConfig = getTransform(transformId);
        assertThat(transformConfig.getSource().getProjectRouting(), equalTo(expectedProjectRouting));
    }
}
