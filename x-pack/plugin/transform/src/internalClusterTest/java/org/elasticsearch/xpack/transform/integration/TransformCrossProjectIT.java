/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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

        createDiceTransform(transformId, transformSrc, expectedProjectRouting);

        var transformConfig = getTransform(transformId);
        assertThat(transformConfig.getSource().getProjectRouting(), equalTo(expectedProjectRouting));
    }

    public void testUpdateTransformWithProjectRouting() throws Exception {
        var transformSrc = "reviews_transform_update_project_routing";
        createSourceIndex(transformSrc);
        indexRandomDiceDoc(transformSrc);

        var transformId = "transform_update_project_routing";

        createDiceTransform(transformId, transformSrc, null);
        assertThat(getTransform(transformId).getSource().getProjectRouting(), is(nullValue()));

        Consumer<String> updateProjectRouting = projectRouting -> updateTransform(
            transformId,
            new TransformConfigUpdate(
                new SourceConfig(new String[] { transformSrc }, QueryConfig.matchAll(), Map.of(), projectRouting),
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );

        var expectedProjectRouting = "_alias:_origin";
        updateProjectRouting.accept(expectedProjectRouting);
        assertThat(getTransform(transformId).getSource().getProjectRouting(), equalTo(expectedProjectRouting));

        var newProjectRouting = "_alias:*";
        updateProjectRouting.accept(newProjectRouting);
        assertThat(getTransform(transformId).getSource().getProjectRouting(), equalTo(newProjectRouting));

        updateProjectRouting.accept(null);
        assertThat(getTransform(transformId).getSource().getProjectRouting(), is(nullValue()));

        // the audit index can take some time to create, so we verify it last
        assertBusy(
            () -> assertThat(
                getAuditMessages(transformId).stream().filter(str -> str.contains("project_routing")).toList(),
                containsInAnyOrder(
                    containsString("project_routing has been set to [_alias:_origin]."),
                    containsString("project_routing updated from [_alias:_origin] to [_alias:*]."),
                    containsString("project_routing [_alias:*] has been removed.")
                )
            )
        );
    }

}
