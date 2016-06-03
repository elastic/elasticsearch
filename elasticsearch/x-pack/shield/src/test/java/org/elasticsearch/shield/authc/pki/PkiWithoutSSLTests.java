/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.pki;

import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;

import java.util.Collections;

import static org.hamcrest.Matchers.is;

@ClusterScope(numClientNodes = 0, supportsDedicatedMasters = false, numDataNodes = 1)
public class PkiWithoutSSLTests extends ShieldIntegTestCase {
    @Override
    public boolean sslTransportEnabled() {
        return false;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put("xpack.security.authc.realms.pki1.type", "pki")
                .put("xpack.security.authc.realms.pki1.order", "0")
                .build();
    }

    public void testThatTransportClientWorks() {
        Client client = internalCluster().transportClient();
        assertGreenClusterState(client);
    }

    public void testThatHttpWorks() throws Exception {
        try (RestClient restClient = restClient()) {
            try (ElasticsearchResponse response = restClient.performRequest("GET", "/_nodes", Collections.emptyMap(), null,
                    new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                            UsernamePasswordToken.basicAuthHeaderValue(ShieldSettingsSource.DEFAULT_USER_NAME,
                                    new SecuredString(ShieldSettingsSource.DEFAULT_PASSWORD.toCharArray()))))) {
                assertThat(response.getStatusLine().getStatusCode(), is(200));
            }
        }
    }
}
