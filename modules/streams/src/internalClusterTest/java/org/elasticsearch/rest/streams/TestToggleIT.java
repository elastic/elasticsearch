/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.streams;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.streams.logs.LogsStreamsActivationToggleAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;

public class TestToggleIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(StreamsPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testLogStreamToggle() throws IOException, ExecutionException, InterruptedException {
        boolean[] testParams = new boolean[] { true, false, true };
        for (boolean enable : testParams) {
            doLogStreamToggleTest(enable);
        }
    }

    private void doLogStreamToggleTest(boolean enable) throws IOException, ExecutionException, InterruptedException {
        LogsStreamsActivationToggleAction.Request request = new LogsStreamsActivationToggleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            enable
        );

        AcknowledgedResponse acknowledgedResponse = client().execute(LogsStreamsActivationToggleAction.INSTANCE, request).get();
        ElasticsearchAssertions.assertAcked(acknowledgedResponse);

        ClusterStateRequest state = new ClusterStateRequest(TEST_REQUEST_TIMEOUT);
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(state).get();
        ProjectMetadata projectMetadata = clusterStateResponse.getState().metadata().getProject(ProjectId.DEFAULT);

        assertThat(projectMetadata.<StreamsMetadata>custom(StreamsMetadata.TYPE).isLogsEnabled(), is(enable));
    }

}
