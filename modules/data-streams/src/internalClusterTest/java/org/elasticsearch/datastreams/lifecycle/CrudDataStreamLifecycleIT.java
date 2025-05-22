/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle;

import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.action.DeleteDataStreamLifecycleAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.putComposableIndexTemplate;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.randomDataLifecycleTemplate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CrudDataStreamLifecycleIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, MockTransportService.TestPlugin.class);
    }

    public void testGetLifecycle() throws Exception {
        DataStreamLifecycle.Template lifecycle = randomDataLifecycleTemplate();
        putComposableIndexTemplate("id1", null, List.of("with-lifecycle*"), null, null, lifecycle);
        putComposableIndexTemplate("id2", null, List.of("without-lifecycle*"), null, null, null);
        {
            String dataStreamName = "with-lifecycle-1";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "with-lifecycle-2";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "without-lifecycle";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }

        // Test retrieving all lifecycles
        {
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "*" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle(), equalTo(lifecycle.toDataStreamLifecycle()));
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertThat(response.getDataStreamLifecycles().get(1).lifecycle(), equalTo(lifecycle.toDataStreamLifecycle()));
            assertThat(response.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("without-lifecycle"));
            assertThat(response.getDataStreamLifecycles().get(2).lifecycle(), is(nullValue()));
            assertThat(response.getRolloverConfiguration(), nullValue());
        }

        // Test retrieving all lifecycles prefixed wildcard
        {
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "with-lifecycle*" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(2));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle(), equalTo(lifecycle.toDataStreamLifecycle()));
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertThat(response.getDataStreamLifecycles().get(1).lifecycle(), is(lifecycle.toDataStreamLifecycle()));
            assertThat(response.getRolloverConfiguration(), nullValue());
        }

        // Test retrieving concrete data streams
        {
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "with-lifecycle-1", "with-lifecycle-2" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(2));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle(), equalTo(lifecycle.toDataStreamLifecycle()));
            assertThat(response.getRolloverConfiguration(), nullValue());
        }

        // Test include defaults
        GetDataStreamLifecycleAction.Request getDataLifecycleRequestWithDefaults = new GetDataStreamLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { "*" }
        ).includeDefaults(true);
        GetDataStreamLifecycleAction.Response responseWithRollover = client().execute(
            GetDataStreamLifecycleAction.INSTANCE,
            getDataLifecycleRequestWithDefaults
        ).get();
        assertThat(responseWithRollover.getDataStreamLifecycles().size(), equalTo(3));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(0).lifecycle(), equalTo(lifecycle.toDataStreamLifecycle()));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(1).lifecycle(), equalTo(lifecycle.toDataStreamLifecycle()));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("without-lifecycle"));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(2).lifecycle(), is(nullValue()));
        assertThat(responseWithRollover.getRolloverConfiguration(), notNullValue());
    }

    public void testPutLifecycle() throws Exception {
        putComposableIndexTemplate("id1", null, List.of("my-data-stream*"), null, null, null);
        // Create index without a lifecycle
        String dataStreamName = "my-data-stream";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            dataStreamName
        );
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        {
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "my-data-stream" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().isEmpty(), equalTo(false));
            GetDataStreamLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle = response.getDataStreamLifecycles().get(0);
            assertThat(dataStreamLifecycle.dataStreamName(), is(dataStreamName));
            assertThat(dataStreamLifecycle.lifecycle(), is(nullValue()));
        }

        // Set lifecycle
        {
            TimeValue dataRetention = randomBoolean() ? null : TimeValue.timeValueMillis(randomMillisUpToYear9999());
            PutDataStreamLifecycleAction.Request putDataLifecycleRequest = new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { "*" },
                dataRetention
            );
            assertThat(
                client().execute(PutDataStreamLifecycleAction.INSTANCE, putDataLifecycleRequest).get().isAcknowledged(),
                equalTo(true)
            );
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "my-data-stream" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(1));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("my-data-stream"));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle().dataRetention(), equalTo(dataRetention));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle().enabled(), equalTo(true));
        }

        // Disable the lifecycle
        {
            TimeValue dataRetention = randomBoolean() ? null : TimeValue.timeValueMillis(randomMillisUpToYear9999());
            PutDataStreamLifecycleAction.Request putDataLifecycleRequest = new PutDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                new String[] { "*" },
                dataRetention,
                false
            );
            assertThat(
                client().execute(PutDataStreamLifecycleAction.INSTANCE, putDataLifecycleRequest).get().isAcknowledged(),
                equalTo(true)
            );
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "my-data-stream" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(1));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("my-data-stream"));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle().dataRetention(), equalTo(dataRetention));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle().enabled(), equalTo(false));
        }
    }

    public void testDeleteLifecycle() throws Exception {
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(randomPositiveTimeValue())
            .buildTemplate();
        putComposableIndexTemplate("id1", null, List.of("with-lifecycle*"), null, null, lifecycle);
        putComposableIndexTemplate("id2", null, List.of("without-lifecycle*"), null, null, null);
        {
            String dataStreamName = "with-lifecycle-1";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "with-lifecycle-2";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "with-lifecycle-3";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                dataStreamName
            );
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }

        // Verify that we have 3 data streams with lifecycles
        {
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "with-lifecycle*" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
        }

        // Remove lifecycle from concrete data stream
        {
            DeleteDataStreamLifecycleAction.Request deleteDataLifecycleRequest = new DeleteDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                new String[] { "with-lifecycle-1" }
            );
            assertThat(
                client().execute(DeleteDataStreamLifecycleAction.INSTANCE, deleteDataLifecycleRequest).get().isAcknowledged(),
                equalTo(true)
            );
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "with-lifecycle*" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
            GetDataStreamLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle = response.getDataStreamLifecycles().get(0);
            assertThat(dataStreamLifecycle.dataStreamName(), is("with-lifecycle-1"));
            assertThat(dataStreamLifecycle.lifecycle(), is(nullValue()));
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertThat(response.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("with-lifecycle-3"));

        }

        // Remove lifecycle from all data streams
        {
            DeleteDataStreamLifecycleAction.Request deleteDataLifecycleRequest = new DeleteDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                new String[] { "*" }
            );
            assertThat(
                client().execute(DeleteDataStreamLifecycleAction.INSTANCE, deleteDataLifecycleRequest).get().isAcknowledged(),
                equalTo(true)
            );
            GetDataStreamLifecycleAction.Request getDataLifecycleRequest = new GetDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { "with-lifecycle*" }
            );
            GetDataStreamLifecycleAction.Response response = client().execute(
                GetDataStreamLifecycleAction.INSTANCE,
                getDataLifecycleRequest
            ).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertThat(response.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("with-lifecycle-3"));

            for (GetDataStreamLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle : response.getDataStreamLifecycles()) {
                assertThat(dataStreamLifecycle.lifecycle(), nullValue());
            }
        }
    }
}
