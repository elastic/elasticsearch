/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.DelegatePkiAuthenticationRequest;
import org.elasticsearch.client.security.DelegatePkiAuthenticationResponse;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

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
        X509Certificate rootCA = readCert("testRootCA.crt");
        RequestOptions.Builder optionsBuilder;
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            DelegatePkiAuthenticationRequest delegatePkiRequest;
            // trust root is optional
            if (randomBoolean()) {
                delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA));
            } else {
                delegatePkiRequest = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate, intermediateCA, rootCA));
            }
            optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                    new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
            DelegatePkiAuthenticationResponse delegatePkiResponse = restClient.security().delegatePkiAuthentication(delegatePkiRequest,
                    optionsBuilder.build());
            optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + delegatePkiResponse.getAccessToken());
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

    public void testDelegatePkiFailure() throws Exception {
        X509Certificate clientCertificate = readCert("testClient.crt");
        X509Certificate intermediateCA = readCert("testIntermediateCA.crt");
        X509Certificate bogusCertificate = readCert("bogus.crt");
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", basicAuthHeaderValue(SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())));
        try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
            // incomplete cert chain
            DelegatePkiAuthenticationRequest delegatePkiRequest1 = new DelegatePkiAuthenticationRequest(Arrays.asList(clientCertificate));
            ElasticsearchStatusException e1 = expectThrows(ElasticsearchStatusException.class,
                    () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest1, optionsBuilder.build()));
            assertThat(e1.getMessage(), is("Elasticsearch exception [type=security_exception, reason=unable to authenticate user"
                    + " [O=org, OU=Elasticsearch, CN=Elasticsearch Test Client] for action [cluster:admin/xpack/security/delegate_pki]]"));

            // swapped order
            DelegatePkiAuthenticationRequest delegatePkiRequest2 = new DelegatePkiAuthenticationRequest(
                    Arrays.asList(intermediateCA, clientCertificate));
            ValidationException e2 = expectThrows(ValidationException.class,
                    () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest2, optionsBuilder.build()));
            assertThat(e2.getMessage(), is("Validation Failed: 1: certificates chain must be ordered;"));

            // bogus certificate
            DelegatePkiAuthenticationRequest delegatePkiRequest3 = new DelegatePkiAuthenticationRequest(Arrays.asList(bogusCertificate));
            ElasticsearchStatusException e3 = expectThrows(ElasticsearchStatusException.class,
                    () -> restClient.security().delegatePkiAuthentication(delegatePkiRequest3, optionsBuilder.build()));
            assertThat(e3.getMessage(), startsWith("Elasticsearch exception [type=security_exception, reason=unable to authenticate user"));
        }
    }

    private X509Certificate readCert(String certName) throws Exception {
        Path path = getDataPath("/org/elasticsearch/xpack/security/action/pki_delegation/" + certName);
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }

}
