/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.TestConnectionNotSupportedException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests {@link AzureStorageProvider#testConnection()} via a real {@link BlobServiceClient} pointed at a
 * {@link MockHttpServer}. Kept separate from {@link AzureStorageProviderTests} so the class-level
 * {@code @SuppressForbidden} and {@code @ThreadLeakFilters} annotations do not affect the simpler
 * provider-level tests that need neither.
 */
@SuppressForbidden(reason = "use a http server")
@ThreadLeakFilters(filters = { AzureReactorThreadFilter.class, AzureStorageProviderConnectionTests.ReactorParallelThreadFilter.class })
public class AzureStorageProviderConnectionTests extends ESTestCase {

    public static final class ReactorParallelThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().startsWith("parallel-");
        }
    }

    /**
     * When {@code getAccountInfo()} returns 403 with {@code x-ms-error-code: AuthorizationPermissionMismatch},
     * {@link AzureStorageProvider#testConnection()} must throw {@link TestConnectionNotSupportedException}
     * with a user-visible message directing the user to create a dataset.
     */
    public void testTestConnectionContainerScoped403IsUntestable() throws IOException {
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().add("x-ms-error-code", "AuthorizationPermissionMismatch");
            exchange.sendResponseHeaders(403, 0);
            exchange.close();
        });
        server.start();
        try {
            BlobServiceClient client = newBlobServiceClient(server);
            AzureStorageProvider provider = new AzureStorageProvider(client);
            TestConnectionNotSupportedException ex = expectThrows(TestConnectionNotSupportedException.class, provider::testConnection);
            assertThat(ex.userReason(), containsString("dataset"));
        } finally {
            server.stop(0);
        }
    }

    /**
     * When {@code getAccountInfo()} returns 403 with {@code x-ms-error-code: AuthenticationFailed},
     * {@link AzureStorageProvider#testConnection()} must NOT catch the exception as untestable —
     * wrong credentials must propagate so the coordinator maps them to {@code failure}.
     * <p>
     * This is the regression guard for the {@code isContainerScoped403} guard: if the guard is
     * removed or widened to all 403s, this test catches the regression.
     */
    public void testTestConnectionAuthenticationFailed403IsFailure() throws IOException {
        HttpServer server = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            exchange.getResponseHeaders().add("x-ms-error-code", "AuthenticationFailed");
            exchange.sendResponseHeaders(403, 0);
            exchange.close();
        });
        server.start();
        try {
            BlobServiceClient client = newBlobServiceClient(server);
            AzureStorageProvider provider = new AzureStorageProvider(client);
            BlobStorageException ex = expectThrows(BlobStorageException.class, provider::testConnection);
            assertEquals(403, ex.getStatusCode());
        } finally {
            server.stop(0);
        }
    }

    private static BlobServiceClient newBlobServiceClient(HttpServer server) {
        String endpoint = "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/devstoreaccount1";
        // Azurite's well-known dev-storage shared key — value comes from public Azure SDK docs.
        // Used here only to satisfy the SDK's auth-header-signing pipeline against our local server.
        String connectionString = "DefaultEndpointsProtocol=http;"
            + "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint="
            + endpoint;
        // maxTries=1: no retries so a 403 surfaces immediately.
        RequestRetryOptions noRetries = new RequestRetryOptions(
            RetryPolicyType.FIXED,
            1,
            (int) Duration.ofSeconds(5).getSeconds(),
            1L,
            1L,
            null
        );
        return new BlobServiceClientBuilder().connectionString(connectionString).retryOptions(noRetries).buildClient();
    }
}
