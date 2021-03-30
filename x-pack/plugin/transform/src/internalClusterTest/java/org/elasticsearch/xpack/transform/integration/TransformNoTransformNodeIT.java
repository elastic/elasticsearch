/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformNoTransformNodeIT extends TransformSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest, remote_cluster_client").build();
    }

    public void testWarningForStats() {
        GetTransformStatsAction.Request getTransformStatsRequest = new GetTransformStatsAction.Request("_all");
        GetTransformStatsAction.Response getTransformStatsResponse = client().execute(
            GetTransformStatsAction.INSTANCE,
            getTransformStatsRequest
        ).actionGet();

        assertEquals(0, getTransformStatsResponse.getTransformsStats().size());

        assertWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
    }

    public void testWarningForGet() {
        GetTransformAction.Request getTransformRequest = new GetTransformAction.Request("_all");
        GetTransformAction.Response getTransformResponse = client().execute(GetTransformAction.INSTANCE, getTransformRequest).actionGet();
        assertEquals(0, getTransformResponse.getTransformConfigurations().size());

        assertWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
    }

    public void testFailureForPreview() {
        String transformId = "transform-with-remote-index";
        TransformConfig config =
            new TransformConfig.Builder()
                .setId(transformId)
                .setSource(new SourceConfig("my-index"))
                .setDest(new DestConfig("my-dest-index", null))
                .setPivotConfig(PivotConfigTests.randomPivotConfig())
                .build();
        PreviewTransformAction.Request request = new PreviewTransformAction.Request(config);
        ElasticsearchStatusException e =
            expectThrows(ElasticsearchStatusException.class, () -> client().execute(PreviewTransformAction.INSTANCE, request).actionGet());
        assertThat(
            e.getMessage(),
            is(equalTo("At least one transform node is required but no transform nodes were found")));
    }

    public void testFailureForPut() {
        String transformId = "transform-with-remote-index";
        TransformConfig config =
            new TransformConfig.Builder()
                .setId(transformId)
                .setSource(new SourceConfig("my-index"))
                .setDest(new DestConfig("my-dest-index", null))
                .setPivotConfig(PivotConfigTests.randomPivotConfig())
                .build();
        PutTransformAction.Request request = new PutTransformAction.Request(config, false);
        ElasticsearchStatusException e =
            expectThrows(
                ElasticsearchStatusException.class,
                () -> client().execute(PutTransformAction.INSTANCE, request).actionGet());
        assertThat(
            e.getMessage(),
            is(equalTo("At least one transform node is required but no transform nodes were found")));
    }
}
