/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.equalTo;

public class SecurityDomainIntegTests extends AbstractProfileIntegTestCase {

    private static final Map<String, Object> REALM_DOMAIN_MAP = Map.of(
        "name",
        "my_domain",
        "realms",
        List.of(Map.of("name", "file", "type", "file"), Map.of("name", "index", "type", "native"))
    );

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // Register both file and native realms under the same domain
        builder.put("xpack.security.authc.domains.my_domain.realms", "file,index");
        return builder.build();
    }

    public void testDomainCaptureForApiKey() {
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest(randomAlphaOfLengthBetween(3, 8), null, null);

        final CreateApiKeyResponse createApiKeyResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD))
        ).execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest).actionGet();

        final XContentTestUtils.JsonMapView getResponseView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(
                client().prepareGet(SECURITY_MAIN_ALIAS, createApiKeyResponse.getId()).execute().actionGet().getSourceAsBytes()
            )
        );

        // domain info is captured
        assertThat(getResponseView.get("creator.realm_domain"), equalTo(REALM_DOMAIN_MAP));

        // API key is usable
        client().filterWithHeader(
            Map.of(
                "Authorization",
                "ApiKey "
                    + Base64.getEncoder()
                        .encodeToString(
                            (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8)
                        )
            )
        ).admin().cluster().prepareHealth().execute().actionGet();
    }

    public void testDomainCaptureForServiceToken() {
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final CreateServiceAccountTokenRequest createServiceTokenRequest = new CreateServiceAccountTokenRequest(
            "elastic",
            "fleet-server",
            tokenName
        );

        final CreateServiceAccountTokenResponse createServiceTokenResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD))
        ).execute(CreateServiceAccountTokenAction.INSTANCE, createServiceTokenRequest).actionGet();

        final XContentTestUtils.JsonMapView responseView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(
                client().prepareGet(SECURITY_MAIN_ALIAS, "service_account_token-elastic/fleet-server/" + tokenName)
                    .execute()
                    .actionGet()
                    .getSourceAsBytes()
            )
        );

        assertThat(responseView.get("creator.realm_domain"), equalTo(REALM_DOMAIN_MAP));

        // The service token is usable
        client().filterWithHeader(Map.of("Authorization", "Bearer " + createServiceTokenResponse.getValue()))
            .admin()
            .cluster()
            .prepareHealth()
            .execute()
            .actionGet();
    }
}
