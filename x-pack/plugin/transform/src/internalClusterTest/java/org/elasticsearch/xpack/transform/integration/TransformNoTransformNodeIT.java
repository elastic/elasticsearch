/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransformNoTransformNodeIT extends TransformSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest")
            // TODO Change this to run with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testGetTransformStats() {
        GetTransformStatsAction.Request request = new GetTransformStatsAction.Request("_all", AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        GetTransformStatsAction.Response response = client().execute(GetTransformStatsAction.INSTANCE, request).actionGet();
        assertThat(response.getTransformsStats(), is(empty()));

        assertCriticalWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
    }

    public void testGetTransform() {
        GetTransformAction.Request request = new GetTransformAction.Request("_all");
        GetTransformAction.Response response = client().execute(GetTransformAction.INSTANCE, request).actionGet();
        assertThat(response.getTransformConfigurations(), is(empty()));

        assertCriticalWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
    }

    public void testPreviewTransform() {
        String transformId = "transform-1";
        TransformConfig config = randomConfig(transformId);
        PreviewTransformAction.Request request = new PreviewTransformAction.Request(config, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(PreviewTransformAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), is(equalTo("Transform requires the transform node role for at least 1 node, found no transform nodes")));
    }

    public void testPutTransform_DeferValidation() {
        String transformId = "transform-2";
        TransformConfig config = randomConfig(transformId);
        PutTransformAction.Request request = new PutTransformAction.Request(config, true, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        AcknowledgedResponse response = client().execute(PutTransformAction.INSTANCE, request).actionGet();
        assertThat(response.isAcknowledged(), is(true));

        assertCriticalWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
    }

    public void testPutTransform_NoDeferValidation() {
        String transformId = "transform-2";
        TransformConfig config = randomConfig(transformId);
        PutTransformAction.Request request = new PutTransformAction.Request(config, false, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(PutTransformAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), is(equalTo("Transform requires the transform node role for at least 1 node, found no transform nodes")));
    }

    public void testUpdateTransform_DeferValidation() {
        String transformId = "transform-3";
        {
            TransformConfig config = randomConfig(transformId);
            PutTransformAction.Request request = new PutTransformAction.Request(config, true, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
            AcknowledgedResponse response = client().execute(PutTransformAction.INSTANCE, request).actionGet();
            assertThat(response.isAcknowledged(), is(true));
        }

        TransformConfigUpdate update = new TransformConfigUpdate(
            new SourceConfig("my-index", "my-index-2"),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        UpdateTransformAction.Request request = new UpdateTransformAction.Request(
            update,
            transformId,
            true,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT
        );
        client().execute(UpdateTransformAction.INSTANCE, request).actionGet();

        assertCriticalWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
    }

    public void testUpdateTransform_NoDeferValidation() {
        String transformId = "transform-3";
        {
            TransformConfig config = randomConfig(transformId);
            PutTransformAction.Request request = new PutTransformAction.Request(config, true, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
            AcknowledgedResponse response = client().execute(PutTransformAction.INSTANCE, request).actionGet();
            assertThat(response.isAcknowledged(), is(true));
            assertCriticalWarnings("Transform requires the transform node role for at least 1 node, found no transform nodes");
        }

        TransformConfigUpdate update = new TransformConfigUpdate(
            new SourceConfig("my-index", "my-index-2"),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        UpdateTransformAction.Request request = new UpdateTransformAction.Request(
            update,
            transformId,
            false,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT
        );
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(UpdateTransformAction.INSTANCE, request).actionGet()
        );
        assertThat(e.getMessage(), is(equalTo("Transform requires the transform node role for at least 1 node, found no transform nodes")));
    }

    private static TransformConfig randomConfig(String transformId) {
        return new TransformConfig.Builder().setId(transformId)
            .setSource(new SourceConfig("my-index"))
            .setDest(new DestConfig("my-dest-index", null, null))
            .setPivotConfig(PivotConfigTests.randomPivotConfig())
            .build();
    }
}
