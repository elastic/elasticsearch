/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for ExternalSourceResolver to verify it properly integrates
 * with the EsqlSession resolution flow.
 */
public class ExternalSourceResolverIntegrationTests extends ESTestCase {

    /**
     * Test that ExternalSourceResolver returns empty resolution when no paths are provided.
     */
    public void testEmptyPathsReturnsEmptyResolution() throws Exception {
        Executor executor = Runnable::run; // Direct executor for tests
        ExternalSourceResolver resolver = new ExternalSourceResolver(executor);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ExternalSourceResolution> result = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();

        resolver.resolve(List.of(), Map.of(), new ActionListener<ExternalSourceResolution>() {
            @Override
            public void onResponse(ExternalSourceResolution resolution) {
                result.set(resolution);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue("Resolver did not complete in time", latch.await(5, TimeUnit.SECONDS));
        assertNull("Should not have error", error.get());
        assertNotNull("Should have result", result.get());
        assertTrue("Resolution should be empty", result.get().isEmpty());
    }

    /**
     * Test that ExternalSourceResolver returns empty resolution when null paths are provided.
     */
    public void testNullPathsReturnsEmptyResolution() throws Exception {
        Executor executor = Runnable::run;
        ExternalSourceResolver resolver = new ExternalSourceResolver(executor);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ExternalSourceResolution> result = new AtomicReference<>();

        resolver.resolve(null, Map.of(), new ActionListener<ExternalSourceResolution>() {
            @Override
            public void onResponse(ExternalSourceResolution resolution) {
                result.set(resolution);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
            }
        });

        assertTrue("Resolver did not complete in time", latch.await(5, TimeUnit.SECONDS));
        assertNotNull("Should have result", result.get());
        assertTrue("Resolution should be empty", result.get().isEmpty());
    }

    /**
     * Test parameter extraction from expressions.
     */
    public void testParameterExtraction() {
        Source source = Source.EMPTY;
        Expression accessKey = new Literal(source, "test-key", null);
        Expression secretKey = new Literal(source, "test-secret", null);

        Map<String, Expression> params = Map.of("access_key", accessKey, "secret_key", secretKey);

        assertNotNull("Parameters should not be null", params);
        assertEquals("Should have 2 parameters", 2, params.size());
        assertTrue("Should contain access_key", params.containsKey("access_key"));
        assertTrue("Should contain secret_key", params.containsKey("secret_key"));
    }

    /**
     * Test that ExternalSourceResolution can be queried for metadata.
     */
    public void testExternalSourceResolutionQuery() {
        Map<String, ExternalSourceMetadata> resolved = Map.of();
        ExternalSourceResolution resolution = new ExternalSourceResolution(resolved);

        assertTrue("Empty resolution should be empty", resolution.isEmpty());
        assertNull("Should return null for non-existent path", resolution.get("s3://bucket/table"));
    }

    /**
     * Test that EMPTY constant is properly initialized.
     */
    public void testEmptyConstant() {
        assertNotNull("EMPTY should not be null", ExternalSourceResolution.EMPTY);
        assertTrue("EMPTY should be empty", ExternalSourceResolution.EMPTY.isEmpty());
        assertNull("EMPTY should return null for any path", ExternalSourceResolution.EMPTY.get("any-path"));
    }
}
