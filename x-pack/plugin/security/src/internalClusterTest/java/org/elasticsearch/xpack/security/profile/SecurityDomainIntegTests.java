/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.token.RefreshTokenAction;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_TOKENS_ALIAS;
import static org.hamcrest.Matchers.arrayWithSize;
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

    public void testDomainCaptureForOAuth2Token() {
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest(
            "password",
            RAC_USER_NAME,
            NATIVE_RAC_USER_PASSWORD.clone(),
            null,
            null,
            null
        );

        final CreateTokenResponse createTokenResponse = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD))
        ).execute(CreateTokenAction.INSTANCE, createTokenRequest).actionGet();

        // Domain info is captured
        assertDomainCapturedForToken();

        // Access token is usable
        client().filterWithHeader(Map.of("Authorization", "Bearer " + createTokenResponse.getTokenString()))
            .admin()
            .cluster()
            .prepareHealth()
            .execute()
            .actionGet();

        // Refresh token is usable
        final CreateTokenRequest refreshTokenRequest = new CreateTokenRequest(
            "refresh_token",
            null,
            null,
            null,
            null,
            createTokenResponse.getRefreshToken()
        );
        client().filterWithHeader(Map.of("Authorization", basicAuthHeaderValue(RAC_USER_NAME, NATIVE_RAC_USER_PASSWORD)))
            .execute(RefreshTokenAction.INSTANCE, refreshTokenRequest)
            .actionGet();

        // Domain info is captured for the refreshed token
        assertDomainCapturedForToken();
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

    private void assertDomainCapturedForToken() {
        final SearchResponse searchResponse = client().prepareSearch(SECURITY_TOKENS_ALIAS)
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("refresh_token.client.user", RAC_USER_NAME))
                    .must(QueryBuilders.termQuery("refresh_token.refreshed", false))
            )
            .execute()
            .actionGet();
        assertThat(searchResponse.getHits().getHits(), arrayWithSize(1));

        final XContentTestUtils.JsonMapView responseView = XContentTestUtils.createJsonMapView(
            new ByteArrayInputStream(searchResponse.getHits().getHits()[0].getSourceAsString().getBytes(StandardCharsets.UTF_8))
        );

        assertThat(responseView.get("access_token.realm_domain"), equalTo(REALM_DOMAIN_MAP));
        assertThat(responseView.get("refresh_token.client.realm_domain"), equalTo(REALM_DOMAIN_MAP));
    }
}
