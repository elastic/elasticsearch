/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.http.PemHttpsConfigurator;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SamlServiceProviderMetadataIT extends ESRestTestCase {

    private static HttpsServer httpsServer;
    private static Map<Integer, Boolean> metadataAvailable = new HashMap<>();

    public static ElasticsearchCluster cluster = initTestCluster();

    private static Path caPath;

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(new RunnableTestRuleAdapter(SamlServiceProviderMetadataIT::initWebserver))
        .around(cluster);

    private static void initWebserver() {
        try {
            final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress().getHostAddress(), 0);
            final Path cert = getDataResource("/ssl/http.crt");
            final Path key = getDataResource("/ssl/http.key");
            httpsServer = MockHttpServer.createHttps(address, 0);
            httpsServer.setHttpsConfigurator(new PemHttpsConfigurator(cert, key, new char[0]));
            httpsServer.start();
            List.of(1, 2, 3).forEach(realmNumber -> {
                try {
                    configureMetadataResource(realmNumber);
                } catch (CertificateException | IOException | URISyntaxException e) {
                    throw new RuntimeException("Cannot configure metadata for realm " + realmNumber, e);
                }
            });
        } catch (URISyntaxException | IOException | GeneralSecurityException e) {
            throw new RuntimeException("Failed to initialise mock web server", e);
        }
    }

    @AfterClass
    public static void shutdownWebserver() {
        httpsServer.stop(0);
        httpsServer = null;
    }

    private static ElasticsearchCluster initTestCluster() {
        return ElasticsearchCluster.local()
            .nodes(1)
            .module("analysis-common")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.security.authc.token.enabled", "true")
            .setting("xpack.security.authc.api_key.enabled", "true")
            .setting("xpack.security.http.ssl.enabled", "true")
            .setting("xpack.security.http.ssl.certificate", "node.crt")
            .setting("xpack.security.http.ssl.key", "node.key")
            .setting("xpack.security.http.ssl.certificate_authorities", "ca.crt")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.certificate", "node.crt")
            .setting("xpack.security.transport.ssl.key", "node.key")
            .setting("xpack.security.transport.ssl.certificate_authorities", "ca.crt")
            .setting("xpack.security.transport.ssl.verification_mode", "certificate")
            .keystore("bootstrap.password", "x-pack-test-password")
            .user("test_admin", "x-pack-test-password", User.ROOT_USER_ROLE, true)
            .user("rest_test", "rest_password", User.ROOT_USER_ROLE, false)
            .configFile("node.key", Resource.fromClasspath("ssl/node.key"))
            .configFile("node.crt", Resource.fromClasspath("ssl/node.crt"))
            .configFile("ca.crt", Resource.fromClasspath("ssl/ca.crt"))
            .settings(node -> {
                var samlWebServerAddress = httpsServer.getAddress();
                var https = "https://" + samlWebServerAddress.getHostName() + ":" + samlWebServerAddress.getPort() + "/";
                var settings = new HashMap<String, String>();
                for (int realmNumber : List.of(1, 2, 3)) {
                    var prefix = "xpack.security.authc.realms.saml.saml" + realmNumber;
                    var idpEntityId = getIdpEntityId(realmNumber);
                    settings.put(prefix + ".order", String.valueOf(realmNumber));
                    settings.put(prefix + ".idp.entity_id", idpEntityId);
                    settings.put(prefix + ".idp.metadata.path", https + "metadata/" + realmNumber + ".xml");
                    settings.put(prefix + ".sp.entity_id", "https://sp" + realmNumber + ".example.org/");
                    settings.put(prefix + ".sp.acs", https + "acs/" + realmNumber);
                    settings.put(prefix + ".attributes.principal", "urn:oid:2.5.4.3");
                    settings.put(prefix + ".ssl.certificate_authorities", "ca.crt");

                }
                return settings;
            })
            .build();
    }

    private static void configureMetadataResource(int realmNumber) throws CertificateException, IOException, URISyntaxException {
        metadataAvailable.putIfAbsent(realmNumber, false);

        var signingCert = getDataResource("/saml/signing.crt");
        var metadataBody = new SamlIdpMetadataBuilder().entityId(getIdpEntityId(realmNumber)).sign(signingCert).asString();
        httpsServer.createContext("/metadata/" + realmNumber + ".xml", http -> {
            if (metadataAvailable.get(realmNumber)) {
                sendXmlContent(metadataBody, http);
            } else {
                if (randomBoolean()) {
                    http.sendResponseHeaders(randomFrom(404, 401, 403, 500), 0);
                } else {
                    sendXmlContent("not valid xml", http);
                }
            }
        });
    }

    private static void sendXmlContent(String bodyContent, HttpExchange http) throws IOException {
        http.getResponseHeaders().add("Content-Type", "text/xml");
        http.sendResponseHeaders(200, bodyContent.length());
        try (var out = http.getResponseBody()) {
            out.write(bodyContent.getBytes(StandardCharsets.UTF_8));
        }
    }

    @BeforeClass
    public static void loadCertificateAuthority() throws Exception {
        URL resource = SamlServiceProviderMetadataIT.class.getResource("/ssl/ca.crt");
        if (resource == null) {
            throw new FileNotFoundException("Cannot find classpath resource /ssl/ca.crt");
        }
        caPath = PathUtils.get(resource.toURI());
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    @Override
    protected Settings restAdminSettings() {
        final String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).put(CERTIFICATE_AUTHORITIES, caPath).build();
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("rest_test", new SecureString("rest_password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).put(CERTIFICATE_AUTHORITIES, caPath).build();
    }

    public void testAuthenticationWhenMetadataIsUnreliable() throws Exception {
        // Start with no metadata available
        assertAllMetadataUnavailable();

        final String username = randomAlphaOfLengthBetween(4, 12);
        for (int realmNumber : shuffledList(List.of(1, 2, 3))) {
            // Authc fails because metadata has never been loaded.
            var ex = expectThrows(ResponseException.class, () -> samlAuthUser(realmNumber, username));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(401));

            // Authc works once metadata is available.
            makeMetadataAvailable(realmNumber);
            samlAuthUser(realmNumber, username);
        }

        // Switch off all metadata
        makeAllMetadataUnavailable();
        for (int realmNumber : List.of(1, 2, 3)) {
            // Authc still works because metadata is cached.
            samlAuthUser(realmNumber, username);
        }
    }

    private void samlAuthUser(int realmNumber, String username) throws Exception {
        var httpsAddress = httpsServer.getAddress();
        var message = new SamlResponseBuilder().spEntityId("https://sp" + realmNumber + ".example.org/")
            .idpEntityId(getIdpEntityId(realmNumber))
            .acs(new URL("https://" + httpsAddress.getHostName() + ":" + httpsAddress.getPort() + "/acs/" + realmNumber))
            .attribute("urn:oid:2.5.4.3", username)
            .sign(getDataPath("/saml/signing.crt"), getDataPath("/saml/signing.key"), new char[0])
            .asString();

        final Map<String, Object> body = new HashMap<>();
        body.put("content", Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
        if (randomBoolean()) {
            // If realm is not specified the action will infer it based on the ACS in the saml auth message
            body.put("realm", "saml" + realmNumber);
        }
        var req = new Request("POST", "_security/saml/authenticate");
        req.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().map(body)));
        var resp = entityAsMap(client().performRequest(req));
        assertThat(resp.get("username"), equalTo(username));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo("saml" + realmNumber));
    }

    private static Path getDataResource(String relativePath) throws URISyntaxException {
        return PathUtils.get(SamlServiceProviderMetadataIT.class.getResource(relativePath).toURI());
    }

    private static String getIdpEntityId(int realmNumber) {
        return "https://idp" + realmNumber + ".example.org/";
    }

    private void makeMetadataAvailable(int... realms) {
        for (int r : realms) {
            metadataAvailable.put(Integer.valueOf(r), true);
        }
    }

    private void assertAllMetadataUnavailable() {
        metadataAvailable.forEach((realm, available) -> assertThat("For realm #" + realm, available, is(false)));
    }

    private void makeAllMetadataUnavailable() {
        metadataAvailable.keySet().forEach(k -> metadataAvailable.put(k, false));
    }

}
