/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.AuthenticateResponse.RealmInfo;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.DelegatePkiRequest;
import org.elasticsearch.xpack.core.security.action.DelegatePkiResponse;
import org.elasticsearch.xpack.security.action.TransportDelegatePkiAction;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
    protected boolean transportSSLEnabled() {
        return true;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testDelegatePki() throws Exception {
        X509Certificate certificate = readCert(getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt"));
        DelegatePkiRequest delegatePkiRequest = new DelegatePkiRequest(new X509Certificate[] { certificate });
        PlainActionFuture<DelegatePkiResponse> future = new PlainActionFuture<>();
        client().execute(TransportDelegatePkiAction.TYPE, delegatePkiRequest, future);
        String token = future.get().getTokenString();
        assertThat(token, is(notNullValue()));
        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", "Bearer " + token);
        RequestOptions options = optionsBuilder.build();
        try(RestHighLevelClient restClient = new TestRestHighLevelClient()) {
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

    static X509Certificate readCert(Path path) throws Exception {
        try (InputStream in = Files.newInputStream(path)) {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
