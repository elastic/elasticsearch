/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction;
import org.elasticsearch.xpack.transform.TransformSingleNodeTestCase;

public class TransformNoTransformNodeIT extends TransformSingleNodeTestCase {
    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master, data, ingest").build();
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
}
