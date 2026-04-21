/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.crud.dataset;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataSourceMetadata;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.metadata.ViewTestsUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.crud.datasource.InMemoryDataSourceService;
import org.elasticsearch.xpack.esql.datasources.crud.datasource.PutDataSourceAction;
import org.elasticsearch.xpack.esql.datasources.crud.datasource.TestDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.indexSettings;
import static org.hamcrest.Matchers.containsString;

public class InMemoryDatasetServiceTests extends ESTestCase {

    private final ProjectId projectId = ProjectId.DEFAULT;
    private InMemoryDataSourceService dataSourceService;
    private InMemoryDatasetService datasetService;

    @Before
    public void setupServices() {
        // Use a validator map that covers both entities: the datasource service dispatches by the request's
        // declared type, while the dataset service dispatches by the parent data source's stored type.
        Map<String, DataSourceValidator> validators = Map.of(
            "s3",
            new TestDataSourceValidator("s3"),
            "gcs",
            new TestDataSourceValidator("gcs")
        );
        dataSourceService = InMemoryDataSourceService.make(validators);
        // Share the ClusterService so both services observe a single consistent cluster state.
        datasetService = InMemoryDatasetService.sharing(dataSourceService.clusterService(), validators);
        // Seed one data source so most tests can put datasets immediately.
        putDataSource("my_s3", "s3", Map.of("region", "us-east-1"));
    }

    @After
    public void teardownServices() {
        dataSourceService.close();
    }

    public void testPutGet() {
        putDataset("access_logs", "my_s3", "s3://bucket/logs/*.parquet", "access log archive", Map.of());
        Dataset stored = datasetService.datasetFor(projectId, "access_logs");
        assertNotNull(stored);
        assertEquals("access_logs", stored.name());
        assertEquals("my_s3", stored.dataSource().getName());
        assertEquals("s3://bucket/logs/*.parquet", stored.resource());
        assertEquals("access log archive", stored.description());
    }

    public void testReplace() {
        putDataset("access_logs", "my_s3", "s3://bucket/logs/", null, Map.of());
        putDataset("access_logs", "my_s3", "s3://bucket/logs-v2/", "updated", Map.of());
        Dataset stored = datasetService.datasetFor(projectId, "access_logs");
        assertEquals("s3://bucket/logs-v2/", stored.resource());
        assertEquals("updated", stored.description());
        assertEquals(Set.of("access_logs"), datasetService.datasetNames(projectId));
    }

    public void testParentMissing() {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.putDataset(projectId, putRequest("ds", "ghost_parent", "s3://x/", null, Map.of()), future);
        ResourceNotFoundException ex = expectThrows(ResourceNotFoundException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("data source [ghost_parent] not found"));
    }

    public void testNameCollisionWithIndex() {
        seedIndex("myindex");
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.putDataset(projectId, putRequest("myindex", "my_s3", "s3://x/", null, Map.of()), future);
        // ensureNoNameCollisions in ProjectMetadata.Builder.build() surfaces an IllegalStateException.
        IllegalStateException ex = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("names need to be unique"));
    }

    public void testNameCollisionWithView() {
        seedView("myview");
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.putDataset(projectId, putRequest("myview", "my_s3", "s3://x/", null, Map.of()), future);
        IllegalStateException ex = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("names need to be unique"));
    }

    public void testMaxCount() {
        // Swap in services with a small cap. Use a fresh shared ClusterService.
        dataSourceService.close();
        Map<String, DataSourceValidator> validators = Map.of("s3", new TestDataSourceValidator("s3"));
        dataSourceService = InMemoryDataSourceService.make(Settings.builder().put("esql.datasets.max_count", 2).build(), validators);
        datasetService = InMemoryDatasetService.sharing(dataSourceService.clusterService(), validators);
        putDataSource("my_s3", "s3", Map.of());
        putDataset("a", "my_s3", "s3://x/", null, Map.of());
        putDataset("b", "my_s3", "s3://y/", null, Map.of());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.putDataset(projectId, putRequest("c", "my_s3", "s3://z/", null, Map.of()), future);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("maximum number of datasets is reached: 2"));
    }

    public void testDelete() {
        putDataset("ds", "my_s3", "s3://x/", null, Map.of());
        assertNotNull(datasetService.datasetFor(projectId, "ds"));
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.deleteDatasets(projectId, TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, java.util.List.of("ds"), future);
        assertTrue(future.actionGet().isAcknowledged());
        assertNull(datasetService.datasetFor(projectId, "ds"));
    }

    public void testDeleteNonExistent() {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.deleteDatasets(projectId, TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, java.util.List.of("ghost"), future);
        ResourceNotFoundException ex = expectThrows(ResourceNotFoundException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("dataset [ghost] not found"));
    }

    public void testGetMissing() {
        assertNull("missing name must return null, not throw", datasetService.datasetFor(projectId, "ghost"));
    }

    /**
     * Companion to IT scenario 6: pins the CAS task body's re-check against a fabricated execute-time
     * state where the parent data source has been deleted between dispatch and task execution. The IT
     * proves the race end-to-end with a real master; this test pins the same semantics without one.
     */
    public void testExecutePutDatasetTaskBodyRejectsMissingParent() {
        final ClusterState dispatchState = datasetService.clusterService().state();
        final ProjectMetadata dispatchProject = dispatchState.metadata().getProject(projectId);
        assertNotNull("precondition: parent exists at dispatch time", DataSourceMetadata.get(dispatchProject).get("my_s3"));

        final ProjectMetadata executeProject = ProjectMetadata.builder(dispatchProject)
            .putCustom(DataSourceMetadata.TYPE, DataSourceMetadata.EMPTY)
            .build();
        final ClusterState executeState = ClusterState.builder(dispatchState).putProjectMetadata(executeProject).build();

        PutDatasetAction.Request request = putRequest("access_logs", "my_s3", "s3://x/", null, Map.of());
        ResourceNotFoundException ex = expectThrows(
            ResourceNotFoundException.class,
            () -> datasetService.executePutDatasetTaskBody(executeState, projectId, request)
        );
        assertThat(ex.getMessage(), containsString("data source [my_s3] not found"));
    }

    /**
     * Joint test: deleting a data source that still has a dataset pointing at it must be rejected with 409.
     * Relies on both services sharing a ClusterService so the data-source side sees the dataset metadata.
     */
    public void testDeleteDataSourceWithDependents() {
        putDataset("ds", "my_s3", "s3://x/", null, Map.of());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        dataSourceService.deleteDataSources(projectId, TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, java.util.List.of("my_s3"), future);
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class, future::actionGet);
        assertEquals(RestStatus.CONFLICT, ex.status());
        assertThat(ex.getMessage(), containsString("referenced by datasets [ds]"));
        // The data source must still be there.
        assertNotNull(dataSourceService.dataSourceFor("my_s3"));
    }

    // ----- helpers -----

    private void putDataSource(String name, String type, Map<String, Object> rawSettings) {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        dataSourceService.putDataSource(
            projectId,
            new PutDataSourceAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name, type, null, rawSettings),
            future
        );
        assertTrue(future.actionGet().isAcknowledged());
    }

    private void putDataset(String name, String dataSource, String resource, String description, Map<String, Object> rawSettings) {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        datasetService.putDataset(projectId, putRequest(name, dataSource, resource, description, rawSettings), future);
        assertTrue("put dataset [" + name + "] did not acknowledge", future.actionGet().isAcknowledged());
    }

    private PutDatasetAction.Request putRequest(
        String name,
        String dataSource,
        String resource,
        String description,
        Map<String, Object> rawSettings
    ) {
        return new PutDatasetAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            name,
            dataSource,
            resource,
            description,
            rawSettings
        );
    }

    /** Directly inject an index into cluster state so the following dataset put collides. */
    private void seedIndex(String name) {
        var cs = datasetService.clusterService();
        ProjectMetadata current = cs.state().metadata().getProject(projectId);
        ProjectMetadata.Builder pb = ProjectMetadata.builder(current)
            .put(IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)));
        ClusterServiceUtils.setState(cs, ClusterState.builder(cs.state()).putProjectMetadata(pb).build());
    }

    /** Directly inject a view into cluster state via {@link ViewTestsUtils}. */
    private void seedView(String name) {
        var cs = datasetService.clusterService();
        ProjectMetadata current = cs.state().metadata().getProject(projectId);
        View view = ViewTestsUtils.randomView(name);
        ViewMetadata existing = current.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
        Map<String, View> views = new java.util.HashMap<>(existing.views());
        views.put(name, view);
        ProjectMetadata.Builder pb = ProjectMetadata.builder(current).putCustom(ViewMetadata.TYPE, new ViewMetadata(views));
        ClusterServiceUtils.setState(cs, ClusterState.builder(cs.state()).putProjectMetadata(pb).build());
    }
}
