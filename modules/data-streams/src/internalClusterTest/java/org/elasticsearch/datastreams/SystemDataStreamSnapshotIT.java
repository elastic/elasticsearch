/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.datastreams.SystemDataStreamSnapshotIT.SystemDataStreamTestPlugin.SYSTEM_DATA_STREAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class SystemDataStreamSnapshotIT extends AbstractSnapshotIntegTestCase {

    public static final String REPO = "repo";
    public static final String SNAPSHOT = "snap";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockRepository.Plugin.class, DataStreamsPlugin.class, SystemDataStreamTestPlugin.class);
    }

    public void testSystemDataStreamInGlobalState() throws Exception {
        Path location = randomRepoPath();
        createRepository(REPO, "fs", location);

        {
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(SYSTEM_DATA_STREAM_NAME);
            final AcknowledgedResponse response = client().execute(CreateDataStreamAction.INSTANCE, request).get();
            assertTrue(response.isAcknowledged());
        }

        // Index a doc so that a concrete backing index will be created
        IndexResponse indexRepsonse = client().prepareIndex(SYSTEM_DATA_STREAM_NAME)
            .setId("42")
            .setSource("{ \"@timestamp\": \"2099-03-08T11:06:07.000Z\", \"name\": \"my-name\" }", XContentType.JSON)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .get();
        assertThat(indexRepsonse.status().getStatus(), oneOf(200, 201));

        {
            GetDataStreamAction.Request request = new GetDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            GetDataStreamAction.Response response = client().execute(GetDataStreamAction.INSTANCE, request).get();
            assertThat(response.getDataStreams(), hasSize(1));
            assertTrue(response.getDataStreams().get(0).getDataStream().isSystem());
        }

        assertSuccessful(
            client().admin()
                .cluster()
                .prepareCreateSnapshot(REPO, SNAPSHOT)
                .setWaitForCompletion(true)
                .setIncludeGlobalState(true)
                .execute()
        );

        // We have to delete the data stream directly, as the feature reset API doesn't clean up system data streams yet
        // See https://github.com/elastic/elasticsearch/issues/75818
        {
            DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            AcknowledgedResponse response = client().execute(DeleteDataStreamAction.INSTANCE, request).get();
            assertTrue(response.isAcknowledged());
        }

        {
            GetIndexResponse indicesRemaining = client().admin().indices().prepareGetIndex().addIndices("_all").get();
            assertThat(indicesRemaining.indices(), arrayWithSize(0));
            assertSystemDataStreamDoesNotExist();
        }

        // Make sure requesting the data stream by name throws.
        // For some reason, expectThrows() isn't working for me here, hence the try/catch.
        try {
            client().admin()
                .cluster()
                .prepareRestoreSnapshot(REPO, SNAPSHOT)
                .setIndices(".test-data-stream")
                .setWaitForCompletion(true)
                .setRestoreGlobalState(randomBoolean()) // this shouldn't matter
                .get();
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(
                e.getMessage(),
                equalTo(
                    "requested system data stream [.test-data-stream], but system data streams can only be restored as part of a feature "
                        + "state"
                )
            );
        }

        assertSystemDataStreamDoesNotExist();

        // Now actually restore the data stream
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setRestoreGlobalState(true)
            .get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());

        {
            GetDataStreamAction.Request request = new GetDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            GetDataStreamAction.Response response = client().execute(GetDataStreamAction.INSTANCE, request).get();
            assertThat(response.getDataStreams(), hasSize(1));
            assertTrue(response.getDataStreams().get(0).getDataStream().isSystem());
        }

        // Attempting to restore again without specifying indices or global/feature states should work, as the wildcard should not be
        // resolved to system indices/data streams.
        client().admin().cluster().prepareRestoreSnapshot(REPO, SNAPSHOT).setWaitForCompletion(true).setRestoreGlobalState(false).get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());
    }

    private void assertSystemDataStreamDoesNotExist() {
        try {
            GetDataStreamAction.Request request = new GetDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            GetDataStreamAction.Response response = client().execute(GetDataStreamAction.INSTANCE, request).get();
            assertThat(response.getDataStreams(), hasSize(0));
        } catch (Exception e) {
            Throwable ex = e;
            while (ex instanceof IndexNotFoundException == false) {
                ex = ex.getCause();
                assertNotNull("expected to find an IndexNotFoundException somewhere in the causes, did not", e);
            }
        }
    }

    public void testSystemDataStreamInFeatureState() throws Exception {
        Path location = randomRepoPath();
        createRepository(REPO, "fs", location);

        {
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(SYSTEM_DATA_STREAM_NAME);
            final AcknowledgedResponse response = client().execute(CreateDataStreamAction.INSTANCE, request).get();
            assertTrue(response.isAcknowledged());
        }

        // Index a doc so that a concrete backing index will be created
        IndexResponse indexToDataStreamResponse = client().prepareIndex(SYSTEM_DATA_STREAM_NAME)
            .setId("42")
            .setSource("{ \"@timestamp\": \"2099-03-08T11:06:07.000Z\", \"name\": \"my-name\" }", XContentType.JSON)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .execute()
            .actionGet();
        assertThat(indexToDataStreamResponse.status().getStatus(), oneOf(200, 201));

        // Index a doc so that a concrete backing index will be created
        IndexResponse indexResponse = client().prepareIndex("my-index")
            .setId("42")
            .setSource("{ \"name\": \"my-name\" }", XContentType.JSON)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .execute()
            .get();
        assertThat(indexResponse.status().getStatus(), oneOf(200, 201));

        {
            GetDataStreamAction.Request request = new GetDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            GetDataStreamAction.Response response = client().execute(GetDataStreamAction.INSTANCE, request).get();
            assertThat(response.getDataStreams(), hasSize(1));
            assertTrue(response.getDataStreams().get(0).getDataStream().isSystem());
        }

        SnapshotInfo snapshotInfo = assertSuccessful(
            client().admin()
                .cluster()
                .prepareCreateSnapshot(REPO, SNAPSHOT)
                .setIndices("my-index")
                .setFeatureStates(SystemDataStreamTestPlugin.class.getSimpleName())
                .setWaitForCompletion(true)
                .setIncludeGlobalState(false)
                .execute()
        );

        assertThat(snapshotInfo.dataStreams(), not(empty()));

        // We have to delete the data stream directly, as the feature reset API doesn't clean up system data streams yet
        // See https://github.com/elastic/elasticsearch/issues/75818
        {
            DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            AcknowledgedResponse response = client().execute(DeleteDataStreamAction.INSTANCE, request).get();
            assertTrue(response.isAcknowledged());
        }

        assertAcked(client().admin().indices().prepareDelete("my-index"));

        {
            GetIndexResponse indicesRemaining = client().admin().indices().prepareGetIndex().addIndices("_all").get();
            assertThat(indicesRemaining.indices(), arrayWithSize(0));
        }

        RestoreSnapshotResponse restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO, SNAPSHOT)
            .setWaitForCompletion(true)
            .setIndices("my-index")
            .setFeatureStates(SystemDataStreamTestPlugin.class.getSimpleName())
            .get();
        assertEquals(restoreSnapshotResponse.getRestoreInfo().totalShards(), restoreSnapshotResponse.getRestoreInfo().successfulShards());

        {
            GetDataStreamAction.Request request = new GetDataStreamAction.Request(new String[] { SYSTEM_DATA_STREAM_NAME });
            GetDataStreamAction.Response response = client().execute(GetDataStreamAction.INSTANCE, request).get();
            assertThat(response.getDataStreams(), hasSize(1));
            assertTrue(response.getDataStreams().get(0).getDataStream().isSystem());
        }
    }

    public static class SystemDataStreamTestPlugin extends Plugin implements SystemIndexPlugin {

        static final String SYSTEM_DATA_STREAM_NAME = ".test-data-stream";

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            return List.of(
                new SystemDataStreamDescriptor(
                    SYSTEM_DATA_STREAM_NAME,
                    "a system data stream for testing",
                    SystemDataStreamDescriptor.Type.EXTERNAL,
                    new ComposableIndexTemplate(
                        List.of(".system-data-stream"),
                        null,
                        null,
                        null,
                        null,
                        null,
                        new ComposableIndexTemplate.DataStreamTemplate()
                    ),
                    Map.of(),
                    Collections.singletonList("test"),
                    new ExecutorNames(ThreadPool.Names.SYSTEM_CRITICAL_READ, ThreadPool.Names.SYSTEM_READ, ThreadPool.Names.SYSTEM_WRITE)
                )
            );
        }

        @Override
        public String getFeatureName() {
            return SystemDataStreamTestPlugin.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "A plugin for testing snapshots of system data streams";
        }
    }
}
