/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.DataSource;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

public class InMemoryDataSourceServiceTests extends ESTestCase {

    private final ProjectId projectId = ProjectId.DEFAULT;
    private InMemoryDataSourceService service;

    @Before
    public void setupService() {
        Map<String, DataSourceValidator> validators = Map.of("s3", new TestDataSourceValidator("s3"));
        service = InMemoryDataSourceService.make(validators);
    }

    @After
    public void teardownService() {
        service.close();
    }

    public void testPutGet() {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        service.putDataSource(
            projectId,
            new PutDataSourceAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "my_s3",
                "s3",
                "prod bucket",
                Map.of("region", "us-east-1", "secret_access_key", "AKIA123")
            ),
            future
        );
        assertTrue(future.actionGet().isAcknowledged());

        DataSource stored = service.get(projectId, "my_s3");
        assertNotNull(stored);
        assertEquals("my_s3", stored.name());
        assertEquals("s3", stored.type());
        assertEquals("prod bucket", stored.description());
        // secret_access_key was wrapped as secret by TestDataSourceValidator (keys starting with "secret_")
        assertTrue(stored.settings().get("secret_access_key").secret());
        assertFalse(stored.settings().get("region").secret());
    }
}
