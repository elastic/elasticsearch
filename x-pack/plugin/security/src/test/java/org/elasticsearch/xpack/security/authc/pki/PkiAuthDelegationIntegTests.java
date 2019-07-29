/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationAction;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationRequest;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class PkiAuthDelegationIntegTests extends SecurityIntegTestCase {

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                .put("xpack.security.authc.realms.pki.pki1.order", "1")
                .putList("xpack.security.authc.realms.pki.pki1.certificate_authorities",
                    getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString())
                .put("xpack.security.authc.realms.pki.pki1.files.role_mapping", getDataPath("role_mapping.yml"))
                .put("xpack.security.authc.realms.pki.pki1.delegation.enabled", true)
                .build();
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(Hasher.resolve(
            randomFrom("pbkdf2", "pbkdf2_1000", "bcrypt", "bcrypt9")).hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() +
            "user_manage:" + usersPasswdHashed + "\n" +
            "user_manage_security:" + usersPasswdHashed + "\n" +
            "user_delegate_pki:" + usersPasswdHashed + "\n" +
            "user_all:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" +
                "role_manage:\n" +
                "  cluster: [ manage ]\n" +
                "\n" +
                "role_manage_security:\n" +
                "  cluster: [ manage_security ]\n" +
                "\n" +
                "role_delegate_pki:\n" +
                "  cluster: [ delegate_pki ]\n" +
                "\n" +
                "role_all:\n" +
                "  cluster: [ all ]\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "\n" +
                "role_manage:user_manage\n" +
                "role_manage_security:user_manage_security\n" +
                "role_delegate_pki:user_delegate_pki\n" +
                "role_all:user_all\n";
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
        final X509Certificate certificate = readCert(
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"));
        final DelegatePkiAuthenticationRequest delegatePkiRequest = new DelegatePkiAuthenticationRequest(
                new X509Certificate[] { certificate });

        for (String delegateeUsername : Arrays.asList("user_all", "user_delegate_pki")) {
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            String token = client.execute(DelegatePkiAuthenticationAction.INSTANCE, delegatePkiRequest).actionGet().getTokenString();
            assertThat(token, is(notNullValue()));
            RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
            optionsBuilder.addHeader("Authorization", "Bearer " + token);
            RequestOptions options = optionsBuilder.build();
            try (RestHighLevelClient restClient = new TestRestHighLevelClient()) {
                AuthenticateResponse resp = restClient.security().authenticate(options);
                User user = resp.getUser();
                assertThat(user, is(notNullValue()));
                assertThat(user.getUsername(), is("Elasticsearch Test Client"));
                RealmInfo authnRealm = resp.getAuthenticationRealm();
                assertThat(authnRealm, is(notNullValue()));
                assertThat(authnRealm.getName(), is("pki1"));
                assertThat(authnRealm.getType(), is("pki"));
            }
        }

        for (String delegateeUsername : Arrays.asList("user_manage", "user_manage_security")) {
            Client client = client().filterWithHeader(Collections.singletonMap("Authorization", UsernamePasswordToken
                    .basicAuthHeaderValue(delegateeUsername, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)));
            ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> {
                client.execute(DelegatePkiAuthenticationAction.INSTANCE, delegatePkiRequest).actionGet();
            });
            assertThat(e.getMessage(), startsWith("action [cluster:admin/xpack/security/delegate_pki] is unauthorized for user"));
        }
    }

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
