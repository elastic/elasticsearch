/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.UpdateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class TransformNoRemoteClusterClientNodeIT extends TransformSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest, transform")
            // TODO Change this to run with security enabled
            // https://github.com/elastic/elasticsearch/issues/75940
            .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
            .build();
    }

    public void testPreviewTransformWithRemoteIndex() {
        String transformId = "transform-with-remote-index";
        TransformConfig config = randomConfig(transformId, "remote_cluster:my-index");
        PreviewTransformAction.Request request = new PreviewTransformAction.Request(config, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(PreviewTransformAction.INSTANCE, request).actionGet()
        );
        assertThat(
            e.getMessage(),
            allOf(
                containsString("No appropriate node to run on"),
                containsString("transform requires a remote connection but the node does not have the remote_cluster_client role")
            )
        );
    }

    public void testPutTransformWithRemoteIndex_DeferValidation() {
        String transformId = "transform-with-remote-index";
        TransformConfig config = randomConfig(transformId, "remote_cluster:my-index");
        PutTransformAction.Request request = new PutTransformAction.Request(config, true, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        client().execute(PutTransformAction.INSTANCE, request).actionGet();
    }

    public void testPutTransformWithRemoteIndex_NoDeferValidation() {
        String transformId = "transform-with-remote-index";
        TransformConfig config = randomConfig(transformId, "remote_cluster:my-index");
        PutTransformAction.Request request = new PutTransformAction.Request(config, false, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(PutTransformAction.INSTANCE, request).actionGet()
        );
        assertThat(
            e.getMessage(),
            allOf(
                containsString("No appropriate node to run on"),
                containsString("transform requires a remote connection but the node does not have the remote_cluster_client role")
            )
        );
    }

    public void testUpdateTransformWithRemoteIndex_DeferValidation() {
        String transformId = "transform-with-local-index";
        {
            TransformConfig config = randomConfig(transformId, "my-index");
            PutTransformAction.Request request = new PutTransformAction.Request(config, true, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
            AcknowledgedResponse response = client().execute(PutTransformAction.INSTANCE, request).actionGet();
            assertThat(response.isAcknowledged(), is(true));
        }

        TransformConfigUpdate update = new TransformConfigUpdate(
            new SourceConfig("remote_cluster:my-index"),
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
    }

    public void testUpdateTransformWithRemoteIndex_NoDeferValidation() {
        String transformId = "transform-with-local-index";
        {
            TransformConfig config = randomConfig(transformId, "my-index");
            PutTransformAction.Request request = new PutTransformAction.Request(config, true, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT);
            AcknowledgedResponse response = client().execute(PutTransformAction.INSTANCE, request).actionGet();
            assertThat(response.isAcknowledged(), is(true));
        }

        TransformConfigUpdate update = new TransformConfigUpdate(
            new SourceConfig("remote_cluster:my-index"),
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
        assertThat(
            e.getMessage(),
            allOf(
                containsString("No appropriate node to run on"),
                containsString("transform requires a remote connection but the node does not have the remote_cluster_client role")
            )
        );
    }

    private static TransformConfig randomConfig(String transformId, String sourceIndex) {
        return new TransformConfig.Builder().setId(transformId)
            .setSource(new SourceConfig(sourceIndex))
            .setDest(new DestConfig("my-dest-index", null, null))
            .setPivotConfig(PivotConfigTests.randomPivotConfig())
            .build();
    }
}
