/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class InMemoryDataSourceServiceTests extends ESTestCase {

    private final ProjectId projectId = ProjectId.DEFAULT;
    private InMemoryDataSourceService service;

    @Before
    public void setupService() {
        Map<String, DataSourceValidator> validators = Map.of(
            "s3",
            new TestDataSourceValidator("s3"),
            "gcs",
            new TestDataSourceValidator("gcs"),
            "bad",
            new TestDataSourceValidator("bad", true)
        );
        service = InMemoryDataSourceService.make(validators);
    }

    @After
    public void teardownService() {
        service.close();
    }

    public void testPutGet() {
        put("my_s3", "s3", "prod bucket", Map.of("region", "us-east-1", "secret_access_key", "AKIA123"));
        DataSource stored = service.dataSourceFor("my_s3");
        assertNotNull(stored);
        assertEquals("my_s3", stored.name());
        assertEquals("s3", stored.type());
        assertEquals("prod bucket", stored.description());
        assertTrue(stored.settings().get("secret_access_key").secret());
        assertFalse(stored.settings().get("region").secret());
    }

    public void testReplace() {
        put("my_s3", "s3", null, Map.of("region", "us-east-1"));
        put("my_s3", "s3", "updated", Map.of("region", "eu-west-1"));
        DataSource stored = service.dataSourceFor("my_s3");
        assertEquals("updated", stored.description());
        assertEquals("eu-west-1", stored.settings().get("region").nonSecretValue());
        // list size stays 1 after replace
        assertEquals(Set.of("my_s3"), service.dataSourceNames());
    }

    public void testReplaceEqualSucceeds() {
        put("my_s3", "s3", null, Map.of("region", "us-east-1"));
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.putDataSource(projectId, putRequest("my_s3", "s3", null, Map.of("region", "us-east-1")), future);
        assertTrue(future.actionGet().isAcknowledged());
    }

    public void testUnknownType() {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.putDataSource(projectId, putRequest("x", "unknown-type", null, Map.of()), future);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("unknown data source type [unknown-type]"));
    }

    public void testValidatorThrows() {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.putDataSource(projectId, putRequest("x", "bad", null, Map.of()), future);
        ValidationException ex = expectThrows(ValidationException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("test-induced data source validation failure"));
    }

    public void testMaxCount() {
        service.close(); // swap in a service with a tiny cap
        service = InMemoryDataSourceService.make(
            Settings.builder().put("esql.data_sources.max_count", 2).build(),
            Map.of("s3", new TestDataSourceValidator("s3"))
        );
        put("a", "s3", null, Map.of());
        put("b", "s3", null, Map.of());
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.putDataSource(projectId, putRequest("c", "s3", null, Map.of()), future);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("maximum number of data sources is reached: 2"));
        assertEquals(Set.of("a", "b"), service.dataSourceNames());
    }

    public void testDelete() {
        put("my_s3", "s3", null, Map.of("region", "us-east-1"));
        assertNotNull(service.dataSourceFor("my_s3"));

        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.deleteDataSources(projectId, TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, java.util.List.of("my_s3"), future);
        assertTrue(future.actionGet().isAcknowledged());
        assertNull(service.dataSourceFor("my_s3"));
    }

    public void testDeleteNonExistent() {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.deleteDataSources(projectId, TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, java.util.List.of("ghost"), future);
        ResourceNotFoundException ex = expectThrows(ResourceNotFoundException.class, future::actionGet);
        assertThat(ex.getMessage(), containsString("data source [ghost] not found"));
    }

    public void testGetMissing() {
        assertNull("missing name must return null, not throw", service.dataSourceFor("ghost"));
    }

    public void testList() {
        put("a", "s3", null, Map.of());
        put("b", "s3", null, Map.of());
        put("c", "gcs", null, Map.of());
        assertEquals(Set.of("a", "b", "c"), service.dataSourceNames());
    }

    // ----- helpers -----

    private void put(String name, String type, String description, Map<String, Object> rawSettings) {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.putDataSource(projectId, putRequest(name, type, description, rawSettings), future);
        assertTrue("put [" + name + "] did not acknowledge", future.actionGet().isAcknowledged());
    }

    private PutDataSourceAction.Request putRequest(String name, String type, String description, Map<String, Object> rawSettings) {
        return new PutDataSourceAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name, type, description, rawSettings);
    }
}
