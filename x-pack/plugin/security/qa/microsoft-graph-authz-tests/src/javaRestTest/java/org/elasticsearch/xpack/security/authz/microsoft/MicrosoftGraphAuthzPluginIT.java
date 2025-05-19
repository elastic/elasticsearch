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
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.security.authc.saml.SamlIdpMetadataBuilder;
import org.elasticsearch.xpack.security.authc.saml.SamlResponseBuilder;
import org.junit.Before;
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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class MicrosoftGraphAuthzPluginIT extends ESRestTestCase {

    private static final String TENANT_ID = "tenant_id";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String USERNAME = "Thor";

    private static final AzureGraphHttpFixture graphFixture = new AzureGraphHttpFixture(
        TENANT_ID,
        CLIENT_ID,
        CLIENT_SECRET,
        USERNAME,
        "Thor Odinson",
        "thor@oldap.test.elasticsearch.com"
    );

    public static ElasticsearchCluster cluster = initTestCluster();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(graphFixture).around(cluster);

    private static final String IDP_ENTITY_ID = "http://idp.example.org/";

    private static ElasticsearchCluster initTestCluster() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
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
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.client_id", CLIENT_ID)
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.client_secret", CLIENT_SECRET)
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.tenant_id", TENANT_ID)
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.graph_host", graphFixture::getBaseUrl)
            .setting("xpack.security.authc.realms.microsoft_graph.microsoft_graph1.access_token_host", graphFixture::getBaseUrl)
            .setting("logger.org.elasticsearch.xpack.security.authz.microsoft", "TRACE")
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

    @Before
    public void setupRoleMapping() throws IOException {
        Request request = new Request("PUT", "/_security/role_mapping/thor-kibana");
        request.setJsonEntity(
            Strings.toString(
                XContentBuilder.builder(XContentType.JSON.xContent())
                    .startObject()
                    .array("roles", new String[] { "microsoft_graph_user" })
                    .field("enabled", true)
                    .startObject("rules")
                    .startArray("all")
                    .startObject()
                    .startObject("field")
                    .field("username", USERNAME)
                    .endObject()
                    .endObject()
                    .startObject()
                    .startObject("field")
                    .field("realm.name", "microsoft_graph1")
                    .endObject()
                    .endObject()
                    .startObject()
                    .startObject("field")
                    .field("groups", "group-id-3")
                    .endObject()
                    .endObject()
                    .endArray() // "all"
                    .endObject() // "rules"
                    .endObject()
            )
        );
        adminClient().performRequest(request);
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
        samlAuthWithMicrosoftGraphAuthz(USERNAME, getSamlAssertionJsonBodyString(USERNAME));
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
        List<String> roles = new XContentTestUtils.JsonMapView(resp).get("authentication.roles");
        assertThat(resp.get("username"), equalTo(username));
        assertThat(roles, contains("microsoft_graph_user"));
        assertThat(ObjectPath.evaluate(resp, "authentication.authentication_realm.name"), equalTo("saml1"));
    }

}
