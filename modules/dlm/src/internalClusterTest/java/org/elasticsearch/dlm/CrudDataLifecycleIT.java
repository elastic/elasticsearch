/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.dlm.action.DeleteDataLifecycleAction;
import org.elasticsearch.dlm.action.GetDataLifecycleAction;
import org.elasticsearch.dlm.action.PutDataLifecycleAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.dlm.DLMFixtures.putComposableIndexTemplate;
import static org.elasticsearch.dlm.DLMFixtures.randomDataLifecycle;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CrudDataLifecycleIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataLifecyclePlugin.class, DataStreamsPlugin.class, MockTransportService.TestPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    public void testGetLifecycle() throws Exception {
        DataLifecycle lifecycle = randomDataLifecycle();
        putComposableIndexTemplate("id1", null, List.of("with-lifecycle*"), null, null, lifecycle);
        putComposableIndexTemplate("id2", null, List.of("without-lifecycle*"), null, null, null);
        {
            String dataStreamName = "with-lifecycle-1";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "with-lifecycle-2";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "without-lifecycle";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }

        // Test retrieving all lifecycles
        {
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "*" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertDataLifecycle(response.getDataStreamLifecycles().get(0).lifecycle(), lifecycle);
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertDataLifecycle(response.getDataStreamLifecycles().get(1).lifecycle(), lifecycle);
            assertThat(response.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("without-lifecycle"));
            assertThat(response.getDataStreamLifecycles().get(2).lifecycle(), nullValue());
            assertThat(response.getRolloverConfiguration(), nullValue());
        }

        // Test retrieving all lifecycles prefixed wildcard
        {
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "with-lifecycle*" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(2));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertDataLifecycle(response.getDataStreamLifecycles().get(0).lifecycle(), lifecycle);
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertDataLifecycle(response.getDataStreamLifecycles().get(1).lifecycle(), lifecycle);
            assertThat(response.getRolloverConfiguration(), nullValue());
        }

        // Test retrieving concrete data streams
        {
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(
                new String[] { "with-lifecycle-1", "with-lifecycle-2" }
            );
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(2));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertDataLifecycle(response.getDataStreamLifecycles().get(0).lifecycle(), lifecycle);
            assertThat(response.getRolloverConfiguration(), nullValue());
        }

        // Test include defaults
        GetDataLifecycleAction.Request getDataLifecycleRequestWithDefaults = new GetDataLifecycleAction.Request(new String[] { "*" })
            .includeDefaults(true);
        GetDataLifecycleAction.Response responseWithRollover = client().execute(
            GetDataLifecycleAction.INSTANCE,
            getDataLifecycleRequestWithDefaults
        ).get();
        assertThat(responseWithRollover.getDataStreamLifecycles().size(), equalTo(3));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
        assertDataLifecycle(responseWithRollover.getDataStreamLifecycles().get(0).lifecycle(), lifecycle);
        assertThat(responseWithRollover.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
        assertDataLifecycle(responseWithRollover.getDataStreamLifecycles().get(1).lifecycle(), lifecycle);
        assertThat(responseWithRollover.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("without-lifecycle"));
        assertThat(responseWithRollover.getDataStreamLifecycles().get(2).lifecycle(), is(nullValue()));
        assertThat(responseWithRollover.getRolloverConfiguration(), notNullValue());
    }

    private void assertDataLifecycle(DataLifecycle dataLifecycle, DataLifecycle expected) {
        if (expected.equals(Template.NO_LIFECYCLE)) {
            assertThat(dataLifecycle, nullValue());
        } else {
            assertThat(dataLifecycle, equalTo(expected));
        }
    }

    public void testPutLifecycle() throws Exception {
        putComposableIndexTemplate("id1", null, List.of("my-data-stream*"), null, null, null);
        // Create index without a lifecycle
        String dataStreamName = "my-data-stream";
        CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
        client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();

        {
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "my-data-stream" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().isEmpty(), equalTo(false));
            GetDataLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle = response.getDataStreamLifecycles().get(0);
            assertThat(dataStreamLifecycle.dataStreamName(), is(dataStreamName));
            assertThat(dataStreamLifecycle.lifecycle(), is(nullValue()));
        }

        // Set lifecycle
        {
            TimeValue dataRetention = randomBoolean() ? null : TimeValue.timeValueMillis(randomMillisUpToYear9999());
            PutDataLifecycleAction.Request putDataLifecycleRequest = new PutDataLifecycleAction.Request(
                new String[] { "*" },
                dataRetention
            );
            assertThat(client().execute(PutDataLifecycleAction.INSTANCE, putDataLifecycleRequest).get().isAcknowledged(), equalTo(true));
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "my-data-stream" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(1));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("my-data-stream"));
            assertThat(response.getDataStreamLifecycles().get(0).lifecycle().getEffectiveDataRetention(), equalTo(dataRetention));
        }
    }

    public void testDeleteLifecycle() throws Exception {
        DataLifecycle lifecycle = new DataLifecycle(randomMillisUpToYear9999());
        putComposableIndexTemplate("id1", null, List.of("with-lifecycle*"), null, null, lifecycle);
        putComposableIndexTemplate("id2", null, List.of("without-lifecycle*"), null, null, null);
        {
            String dataStreamName = "with-lifecycle-1";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "with-lifecycle-2";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }
        {
            String dataStreamName = "with-lifecycle-3";
            CreateDataStreamAction.Request createDataStreamRequest = new CreateDataStreamAction.Request(dataStreamName);
            client().execute(CreateDataStreamAction.INSTANCE, createDataStreamRequest).get();
        }

        // Verify that we have 3 data streams with lifecycles
        {
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "with-lifecycle*" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
        }

        // Remove lifecycle from concrete data stream
        {
            DeleteDataLifecycleAction.Request deleteDataLifecycleRequest = new DeleteDataLifecycleAction.Request(
                new String[] { "with-lifecycle-1" }
            );
            assertThat(
                client().execute(DeleteDataLifecycleAction.INSTANCE, deleteDataLifecycleRequest).get().isAcknowledged(),
                equalTo(true)
            );
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "with-lifecycle*" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
            GetDataLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle = response.getDataStreamLifecycles().get(0);
            assertThat(dataStreamLifecycle.dataStreamName(), is("with-lifecycle-1"));
            assertThat(dataStreamLifecycle.lifecycle(), is(nullValue()));
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertThat(response.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("with-lifecycle-3"));

        }

        // Remove lifecycle from all data streams
        {
            DeleteDataLifecycleAction.Request deleteDataLifecycleRequest = new DeleteDataLifecycleAction.Request(new String[] { "*" });
            assertThat(
                client().execute(DeleteDataLifecycleAction.INSTANCE, deleteDataLifecycleRequest).get().isAcknowledged(),
                equalTo(true)
            );
            GetDataLifecycleAction.Request getDataLifecycleRequest = new GetDataLifecycleAction.Request(new String[] { "with-lifecycle*" });
            GetDataLifecycleAction.Response response = client().execute(GetDataLifecycleAction.INSTANCE, getDataLifecycleRequest).get();
            assertThat(response.getDataStreamLifecycles().size(), equalTo(3));
            assertThat(response.getDataStreamLifecycles().get(0).dataStreamName(), equalTo("with-lifecycle-1"));
            assertThat(response.getDataStreamLifecycles().get(1).dataStreamName(), equalTo("with-lifecycle-2"));
            assertThat(response.getDataStreamLifecycles().get(2).dataStreamName(), equalTo("with-lifecycle-3"));

            for (GetDataLifecycleAction.Response.DataStreamLifecycle dataStreamLifecycle : response.getDataStreamLifecycles()) {
                assertThat(dataStreamLifecycle.lifecycle(), nullValue());
            }
        }
    }
}
