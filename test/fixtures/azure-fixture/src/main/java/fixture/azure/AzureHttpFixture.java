/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.azure;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.elasticsearch.test.ESTestCase.fail;
import static org.hamcrest.Matchers.hasSize;

public class AzureHttpFixture extends ExternalResource {

    private final Protocol protocol;
    private final String account;
    private final String container;
    private final String clientId;
    private final String tenantId;
    private final Predicate<String> authHeaderPredicate;

    private HttpServer server;
    private HttpServer metadataServer;
    private HttpServer oauthTokenServiceServer;

    // JWT-looking value for workload identity authentication -- the exact value has no inherent meaning
    private final String federatedToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9";

    public enum Protocol {
        NONE,
        HTTP,
        HTTPS
    }

    /**
     * @param  account The name of the Azure Blob Storage account against which the request should be authorized..
     * @return a predicate that matches the {@code Authorization} HTTP header that the Azure SDK sends when using shared key auth (i.e.
     *         using a key or SAS token).
     * @see <a href="https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key">Azure docs on shared key auth</a>
     */
    public static Predicate<String> sharedKeyForAccountPredicate(String account) {
        return new Predicate<>() {
            @Override
            public boolean test(String s) {
                return s.startsWith("SharedKey " + account + ":");
            }

            @Override
            public String toString() {
                return "SharedKey[" + account + "]";
            }
        };
    }

    /**
     * A sentinel value for {@code #authHeaderPredicate} which indicates that requests should be authorized by a bearer token provided
     * by the managed identity service.
     */
    public static final Predicate<String> MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE = s -> fail(null, "should not be called");

    /**
     * A sentinel value for {@code #authHeaderPredicate} which indicates that requests should be authorized by a bearer token provided
     * by the workload identity service.
     */
    public static final Predicate<String> WORK_IDENTITY_BEARER_TOKEN_PREDICATE = s -> fail(null, "should not be called");

    /**
     * @param bearerToken The bearer token to accept
     * @return a predicate that matches the {@code Authorization} HTTP header that the Azure SDK sends when using bearer token auth (i.e.
     *         using Managed Identity)
     */
    private static Predicate<String> bearerTokenPredicate(String bearerToken) {
        return new Predicate<>() {
            @Override
            public boolean test(String s) {
                return s.equals("Bearer " + bearerToken);
            }

            @Override
            public String toString() {
                return "Bearer[" + bearerToken + "]";
            }
        };
    }

    public AzureHttpFixture(
        Protocol protocol,
        String account,
        String container,
        @Nullable String tenantId,
        @Nullable String clientId,
        Predicate<String> authHeaderPredicate
    ) {
        if ((clientId == null) != (tenantId == null)) {
            fail(null, "either both [tenantId] and [clientId] must be set or neither must be set");
        }
        if (tenantId != null) {
            if (protocol != Protocol.HTTPS) {
                fail(null, "when [tenantId] is set, protocol must be HTTPS");
            }
        }
        this.protocol = protocol;
        this.account = account;
        this.container = container;
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.authHeaderPredicate = authHeaderPredicate;
    }

    private String scheme() {
        return switch (protocol) {
            case NONE -> fail(null, "fixture is disabled");
            case HTTP -> "http";
            case HTTPS -> "https";
        };
    }

    public String getAddress() {
        return scheme() + "://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/" + account;
    }

    public String getOAuthTokenServiceAddress() {
        return scheme()
            + "://"
            + oauthTokenServiceServer.getAddress().getHostString()
            + ":"
            + oauthTokenServiceServer.getAddress().getPort()
            + "/";
    }

    public String getMetadataAddress() {
        return "http://" + metadataServer.getAddress().getHostString() + ":" + metadataServer.getAddress().getPort() + "/";
    }

    public String getFederatedToken() {
        return federatedToken;
    }

    @Override
    protected void before() {
        try {
            // Set different bearer tokens for managed and workload identity to ensure that we are using the expected one for access
            final var managedIdentityBearerToken = ESTestCase.randomIdentifier();
            final var workloadIdentityBearerToken = ESTestCase.randomIdentifier();

            final Predicate<String> actualAuthHeaderPredicate;
            if (authHeaderPredicate == MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE) {
                actualAuthHeaderPredicate = bearerTokenPredicate(managedIdentityBearerToken);
            } else if (authHeaderPredicate == WORK_IDENTITY_BEARER_TOKEN_PREDICATE) {
                actualAuthHeaderPredicate = bearerTokenPredicate(workloadIdentityBearerToken);
            } else {
                actualAuthHeaderPredicate = authHeaderPredicate;
            }

            if (protocol != Protocol.NONE) {
                this.metadataServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                metadataServer.createContext("/", new AzureMetadataServiceHttpHandler(managedIdentityBearerToken));
                metadataServer.start();
            }

            switch (protocol) {
                case NONE -> {
                }
                case HTTP -> {
                    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    server.createContext("/" + account, new AzureHttpHandler(account, container, actualAuthHeaderPredicate));
                    server.start();

                    oauthTokenServiceServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    oauthTokenServiceServer.createContext(
                        "/",
                        new AzureOAuthTokenServiceHttpHandler(workloadIdentityBearerToken, federatedToken, tenantId, clientId)
                    );
                    oauthTokenServiceServer.start();
                }
                case HTTPS -> {
                    final var tmpdir = ESTestCase.createTempDir();
                    final var certificates = PemUtils.readCertificates(List.of(copyResource(tmpdir, "azure-http-fixture.pem")));
                    assertThat(certificates, hasSize(1));
                    final SSLContext sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(
                        new KeyManager[] {
                            KeyStoreUtil.createKeyManager(
                                new Certificate[] { certificates.get(0) },
                                PemUtils.readPrivateKey(copyResource(tmpdir, "azure-http-fixture.key"), () -> null),
                                null
                            ) },
                        null,
                        new SecureRandom()
                    );
                    {
                        final var httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                        this.server = httpsServer;
                        httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
                        httpsServer.createContext("/" + account, new AzureHttpHandler(account, container, actualAuthHeaderPredicate));
                        httpsServer.start();
                    }
                    {
                        final var httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                        this.oauthTokenServiceServer = httpsServer;
                        httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
                        httpsServer.createContext(
                            "/",
                            new AzureOAuthTokenServiceHttpHandler(workloadIdentityBearerToken, federatedToken, tenantId, clientId)
                        );
                        httpsServer.start();
                    }
                }
            }
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    private Path copyResource(Path tmpdir, String name) throws IOException {
        try (
            var stream = Objects.requireNonNullElseGet(
                getClass().getResourceAsStream(name),
                () -> ESTestCase.fail(null, "resource [%s] not found", name)
            )
        ) {
            final var path = tmpdir.resolve(name);
            Files.write(path, stream.readAllBytes());
            return path;
        }
    }

    @Override
    protected void after() {
        if (protocol != Protocol.NONE) {
            server.stop(0);
            metadataServer.stop(0);
            oauthTokenServiceServer.stop(0);
        }
    }
}
