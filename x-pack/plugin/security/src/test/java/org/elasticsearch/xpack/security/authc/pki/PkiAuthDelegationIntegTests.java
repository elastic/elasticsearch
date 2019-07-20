/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PkiAuthDelegationIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                // pki1 does not allow delegation
                .put("xpack.security.authc.realms.pki.pki1.order", "1")
                .putList("xpack.security.authc.realms.pki.pki1.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString())
                .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
                // pki2 allows delegation but has a non-matching username pattern 
                .put("xpack.security.authc.realms.pki.pki2.order", "2")
                .putList("xpack.security.authc.realms.pki.pki2.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString())
                .put("xpack.security.authc.realms.pki.pki2.username_pattern", "CN=MISMATCH(.*?)(?:,|$)")
                .put("xpack.security.authc.realms.pki.pki2.delegation.enabled", true)
                .put("xpack.security.authc.realms.pki.pki2.files.role_mapping", getDataPath("role_mapping.yml"))
                // pki3 allows delegation and the username pattern (default) matches
                .put("xpack.security.authc.realms.pki.pki3.order", "3")
                .putList("xpack.security.authc.realms.pki.pki3.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/testRootCA.crt").toString())
                .put("xpack.security.authc.realms.pki.pki3.delegation.enabled", true)
                .put("xpack.security.authc.realms.pki.pki3.files.role_mapping", getDataPath("role_mapping.yml"))
                .build();
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testDelegatePki() throws Exception {
        X509Certificate clientCertificate = readCert("testClient.crt");
        X509Certificate intermediateCA = readCert("testIntermediateCA.crt");
        DelegatePkiAuthenticationRequest delegatePkiRequest = new DelegatePkiAuthenticationRequest(
                new X509Certificate[] { clientCertificate, intermediateCA });

        DelegatePkiAuthenticationResponse response = callDelegatePkiAuthentication(delegatePkiRequest);

        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", "Bearer " + response.getTokenString());
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            AuthenticateResponse resp = restClient.security().authenticate(optionsBuilder.build());
            User user = resp.getUser();
            assertThat(user, is(notNullValue()));
            assertThat(user.getUsername(), is("Elasticsearch Test Client"));
            RealmInfo authnRealm = resp.getAuthenticationRealm();
            assertThat(authnRealm, is(notNullValue()));
            assertThat(authnRealm.getName(), is("pki3"));
            assertThat(authnRealm.getType(), is("pki"));
        }
    }

    private X509Certificate readCert(String certName) throws Exception {
        Path path = getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/" + certName);
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }

    private DelegatePkiAuthenticationResponse callDelegatePkiAuthentication(DelegatePkiAuthenticationRequest delegatePkiRequest) throws Exception {
        Request httpRequest = new Request("POST", "/_security/delegate_pki");
        RequestOptions.Builder options = httpRequest.getOptions().toBuilder();
        options.addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
        httpRequest.setOptions(options);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        delegatePkiRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        httpRequest.setJsonEntity(Strings.toString(builder));
        Response rawHttpResponse = getRestClient().performRequest(httpRequest);
        XContentParser responseContent = createParser(JsonXContent.jsonXContent, EntityUtils.toString(rawHttpResponse.getEntity()));
        return DelegatePkiAuthenticationResponse.PARSER.apply(responseContent, null);
    }

}
