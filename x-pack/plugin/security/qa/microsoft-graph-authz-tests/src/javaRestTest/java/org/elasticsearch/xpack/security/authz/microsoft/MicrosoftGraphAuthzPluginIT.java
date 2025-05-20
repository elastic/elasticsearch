/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.security.authc.saml.SamlIdpMetadataBuilder;
import org.elasticsearch.xpack.security.authc.saml.SamlResponseBuilder;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class MicrosoftGraphAuthzPluginIT extends ESRestTestCase {
    public static ElasticsearchCluster cluster = initTestCluster();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(cluster);

    private static final String IDP_ENTITY_ID = "http://idp.example.org/";

    private static ElasticsearchCluster initTestCluster() {
        return ElasticsearchCluster.local()
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.authc.token.enabled", "true")
            .setting("xpack.security.http.ssl.enabled", "false")
            .plugin("microsoft-graph-authz")
            .keystore("bootstrap.password", "x-pack-test-password")
            .user("test_admin", "x-pack-test-password", User.ROOT_USER_ROLE, true)
            .user("rest_test", "rest_password", User.ROOT_USER_ROLE, true)
            .configFile("metadata.xml", Resource.fromString(getIDPMetadata()))
            .setting("xpack.security.authc.realms.saml.saml1.order", "1")
            .setting("xpack.security.authc.realms.saml.saml1.idp.entity_id", IDP_ENTITY_ID)
            .setting("xpack.security.authc.realms.saml.saml1.idp.metadata.path", "metadata.xml")
            .setting("xpack.security.authc.realms.saml.saml1.attributes.principal", "urn:oid:2.5.4.3")
            .setting("xpack.security.authc.realms.saml.saml1.sp.entity_id", "http://sp/default.example.org/")
            .setting("xpack.security.authc.realms.saml.saml1.sp.acs", "http://acs/default")
            .setting("xpack.security.authc.realms.saml.saml1.sp.logout", "http://logout/default")
            .setting("xpack.security.authc.realms.saml.saml1.authorization_realms", "microsoft_graph1")
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.order", "2")
            .build();
    }

    private static String getIDPMetadata() {
        try {
            var signingCert = PathUtils.get(MicrosoftGraphAuthzPluginIT.class.getResource("/saml/signing.crt").toURI());
            return new SamlIdpMetadataBuilder().entityId(IDP_ENTITY_ID).idpUrl(IDP_ENTITY_ID).sign(signingCert).asString();
        } catch (URISyntaxException | CertificateException | IOException exception) {
            fail(exception);
        }
        return null;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getProtocol() {
        return "http";
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("rest_test", new SecureString("rest_password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    public void testAuthenticationSuccessful() throws Exception {
        final String username = randomAlphaOfLengthBetween(4, 12);
        samlAuthWithMicrosoftGraphAuthz(username, getSamlAssertionJsonBodyString(username));
    }

    private String getSamlAssertionJsonBodyString(String username) throws Exception {
        var message = new SamlResponseBuilder().spEntityId("http://sp/default.example.org/")
            .idpEntityId(IDP_ENTITY_ID)
            .acs(new URL("http://acs/default"))
            .attribute("urn:oid:2.5.4.3", username)
            .sign(getDataPath("/saml/signing.crt"), getDataPath("/saml/signing.key"), new char[0])
            .asString();

        final Map<String, Object> body = new HashMap<>();
        body.put("content", Base64.getEncoder().encodeToString(message.getBytes(StandardCharsets.UTF_8)));
        body.put("realm", "saml1");
        return Strings.toString(JsonXContent.contentBuilder().map(body));
    }

    private void samlAuthWithMicrosoftGraphAuthz(String username, String samlAssertion) throws Exception {
        var req = new Request("POST", "_security/saml/authenticate");
        req.setJsonEntity(samlAssertion);
        var resp = entityAsMap(client().performRequest(req));
        List<String> roles = new XContentTestUtils.JsonMapView(entityAsMap(client().performRequest(req))).get("authentication.roles");
        assertThat(resp.get("username"), equalTo(username));
        // TODO add check for mapped groups and roles when available
        assertThat(roles, empty());
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo("saml1"));
    }

}
