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
    private final String tenantId;
    private final Predicate<String> authHeaderPredicate;

    private HttpServer server;
    private HttpServer metadataServer;

    private String federatedTokenPath;

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

    // TODO remove constructor
    public AzureHttpFixture(Protocol protocol, String account, String container, Predicate<String> authHeaderPredicate) {
        this(protocol, account, container, "wrong-bad-no", authHeaderPredicate);
    }

    public AzureHttpFixture(Protocol protocol, String account, String container, String tenantId, Predicate<String> authHeaderPredicate) {
        this.protocol = protocol;
        this.account = account;
        this.container = container;
        this.tenantId = tenantId;
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

    public String getMetadataAddress() {
        return scheme() + "://" + metadataServer.getAddress().getHostString() + ":" + metadataServer.getAddress().getPort() + "/";
    }

    public String getFederatedTokenPath() {
        return federatedTokenPath;
    }

    @Override
    protected void before() {
        try {
            final var bearerToken = ESTestCase.randomIdentifier();

            setupFederatedTokenFile();

            if (protocol != Protocol.NONE) {
                if (protocol == Protocol.HTTPS) {
                    final var httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    this.metadataServer = httpsServer;
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
                    httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
                    httpsServer.createContext("/", new AzureMetadataServiceHttpHandler(tenantId, bearerToken));
                    httpsServer.start();
                } else {
                    this.metadataServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    metadataServer.createContext("/", new AzureMetadataServiceHttpHandler(tenantId, bearerToken));
                    metadataServer.start();
                }
            }

            final var actualAuthHeaderPredicate = authHeaderPredicate == MANAGED_IDENTITY_BEARER_TOKEN_PREDICATE
                ? bearerTokenPredicate(bearerToken)
                : authHeaderPredicate;

            switch (protocol) {
                case NONE -> {}
                case HTTP -> {
                    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    server.createContext("/" + account, new AzureHttpHandler(account, container, actualAuthHeaderPredicate));
                    server.start();
                }
                case HTTPS -> {
                    final var httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                    this.server = httpsServer;
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
                    httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext));
                    httpsServer.createContext("/" + account, new AzureHttpHandler(account, container, actualAuthHeaderPredicate));
                    httpsServer.start();
                }
            }
        } catch (Exception e) {
            throw new AssertionError("unexpected", e);
        }
    }

    private void setupFederatedTokenFile() throws IOException {
        final var tmpdir = ESTestCase.createTempDir();
        federatedTokenPath = copyResource(tmpdir, "azure-federated-token").toString();
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
        }
    }
}
