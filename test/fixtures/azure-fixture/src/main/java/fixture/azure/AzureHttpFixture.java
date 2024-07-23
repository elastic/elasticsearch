/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package fixture.azure;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class AzureHttpFixture extends ExternalResource {

    private final boolean enabled;
    private final String account;
    private final String container;
    private HttpsServer server;

    public AzureHttpFixture(boolean enabled, String account, String container) {
        this.enabled = enabled;
        this.account = account;
        this.container = container;
    }

    public String getAddress() {
        return "https://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/" + account;
    }

    @Override
    protected void before() throws Exception {
        if (enabled) {
            this.server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            final var certificates = PemUtils.readCertificates(
                List.of(ESTestCase.getResourceDataPath(getClass(), "azure-http-fixture.pem"))
            );
            assertThat(certificates, hasSize(1));
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(
                new KeyManager[] {
                    KeyStoreUtil.createKeyManager(
                        new Certificate[] { certificates.get(0) },
                        PemUtils.readPrivateKey(ESTestCase.getResourceDataPath(getClass(), "azure-http-fixture.key"), () -> null),
                        null
                    ) },
                null,
                new SecureRandom()
            );
            server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
            server.createContext("/" + account, new AzureHttpHandler(account, container));
            server.start();
        }
    }

    @Override
    protected void after() {
        if (enabled) {
            server.stop(0);
        }
    }
}
