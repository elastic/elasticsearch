/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.operator.action.OperatorClusterUpdateSettingsAction;
import org.elasticsearch.operator.service.OperatorClusterStateController;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OperatorClusterStateControllerTests extends ESTestCase {

    public void testOperatorController() throws IOException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        final ClusterName clusterName = new ClusterName("elasticsearch");

        ClusterState state = ClusterState.builder(clusterName).build();
        when(clusterService.state()).thenReturn(state);

        OperatorClusterStateController controller = new OperatorClusterStateController(clusterService);
        controller.initHandlers(List.of(new OperatorClusterUpdateSettingsAction(clusterSettings)));

        String testJSON = """
            {
                 "metadata": {
                     "version": "1234",
                     "compatibility": "8.4.0"
                 },
                 "state": {
                     "cluster_settings": {
                         "indices.recovery.max_bytes_per_sec": "50mb"
                     }
                 }
            }
            """;

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, testJSON)) {
            controller.process("operator", parser);
        }
    }
}
