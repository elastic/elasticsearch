/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for AzureStorageObject async wiring and AzureStorageProvider lifecycle.
 * <p>
 * Uses real Azure SDK clients built against a dummy Azurite-style connection string.
 * Final Azure SDK classes cannot be mocked with standard Mockito, so we follow
 * the same pattern as {@code repository-azure}: build real clients, test wiring
 * and validation without hitting the network.
 * <p>
 * Only the sync client is built here (no Reactor Netty threads). Tests that need
 * the async client use the provider which manages its own lifecycle.
 */
public class AzureStorageObjectAsyncTests extends ESTestCase {

    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=http;"
        + "AccountName=devstoreaccount1;"
        + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1";

    private static final StoragePath PATH = StoragePath.of("wasbs://devstoreaccount1.blob.core.windows.net/container/blob.parquet");

    private BlobClient blobClient() {
        return new BlobServiceClientBuilder().connectionString(CONNECTION_STRING)
            .buildClient()
            .getBlobContainerClient("container")
            .getBlobClient("blob.parquet");
    }

    private BlobAsyncClient blobAsyncClient() {
        return new BlobServiceClientBuilder().connectionString(CONNECTION_STRING)
            .buildAsyncClient()
            .getBlobContainerAsyncClient("container")
            .getBlobAsyncClient("blob.parquet");
    }

    public void testSupportsNativeAsyncWithAsyncClient() {
        AzureStorageObject obj = new AzureStorageObject(blobClient(), blobAsyncClient(), "container", "blob.parquet", PATH);
        assertTrue(obj.supportsNativeAsync());
    }

    public void testSupportsNativeAsyncWithoutAsyncClient() {
        AzureStorageObject obj = new AzureStorageObject(blobClient(), "container", "blob.parquet", PATH);
        assertFalse(obj.supportsNativeAsync());
    }

    public void testReadBytesAsyncNegativePositionFails() throws Exception {
        AzureStorageObject obj = new AzureStorageObject(blobClient(), blobAsyncClient(), "container", "blob.parquet", PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(-1, 10, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(ByteBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("position must be non-negative"));
    }

    public void testReadBytesAsyncNegativeLengthFails() throws Exception {
        AzureStorageObject obj = new AzureStorageObject(blobClient(), blobAsyncClient(), "container", "blob.parquet", PATH);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        obj.readBytesAsync(0, -1, Runnable::run, new ActionListener<>() {
            @Override
            public void onResponse(ByteBuffer buffer) {
                fail("expected failure");
            }

            @Override
            public void onFailure(Exception e) {
                error.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(error.get(), instanceOf(IllegalArgumentException.class));
        assertThat(error.get().getMessage(), containsString("length must be positive"));
    }

    public void testProviderCloseDoesNotThrow() throws IOException {
        AzureConfiguration azureConfig = AzureConfiguration.fromFields(
            null,
            "devstoreaccount1",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            null,
            "http://127.0.0.1:10000/devstoreaccount1"
        );
        AzureStorageProvider provider = new AzureStorageProvider(azureConfig);
        provider.close();
    }

    public void testProviderCreatedObjectsAreAsyncCapable() throws IOException {
        AzureConfiguration azureConfig = AzureConfiguration.fromFields(
            null,
            "devstoreaccount1",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            null,
            "http://127.0.0.1:10000/devstoreaccount1"
        );
        try (AzureStorageProvider provider = new AzureStorageProvider(azureConfig)) {
            var obj = provider.newObject(PATH);
            assertTrue("Provider-created objects should support native async", obj.supportsNativeAsync());
        }
    }
}
