/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.profile.SyncProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.SyncProfileRequest;
import org.junit.Before;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SecurityProfileSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String RAC_USER_NAME = "rac_user";

    @Override
    protected String configUsers() {
        return super.configUsers() + RAC_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "rac_role:\n" + "  cluster:\n" + "    - 'manage_own_api_key'\n" + "    - 'monitor'\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "rac_role:" + RAC_USER_NAME + "\n";
    }

    @Before
    public void prepare() {
        final IndexResponse indexResponse = client().prepareIndex(SecurityProfileIndex.SECURITY_PROFILE_INDEX_ALIAS)
            .setSource(
                Map.of(
                    "uid",
                    "u_abcd",
                    "user",
                    Map.of(
                        "username",
                        "foo",
                        "email",
                        "foo@example.com",
                        "full_name",
                        "User Foo",
                        "display_name",
                        "User Foo",
                        "realm",
                        Map.of("name", "native1", "type", "native", "domain", "rac")
                    ),
                    "last_synchronized",
                    Instant.now().toEpochMilli(),
                    "access",
                    Map.of("roles", List.of("rac_role"), "kibana", Map.of("spaces", List.of("default"))),
                    "application_data",
                    Map.of()
                )
            )
            .get();
        assertThat(indexResponse.status().getStatus(), equalTo(201));
    }

    public void testProfileIndexCreated() {
        final GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(SecurityProfileIndex.SECURITY_PROFILE_INDEX_NAME);

        final GetIndexResponse getIndexResponse = client().execute(GetIndexAction.INSTANCE, getIndexRequest).actionGet();

        assertThat(getIndexResponse.getIndices(), arrayContaining(SecurityProfileIndex.SECURITY_PROFILE_INDEX_NAME));
        final List<AliasMetadata> aliases = getIndexResponse.getAliases().get(SecurityProfileIndex.SECURITY_PROFILE_INDEX_NAME);
        assertThat(aliases, hasSize(1));
        assertThat(aliases.get(0).alias(), equalTo(SecurityProfileIndex.SECURITY_PROFILE_INDEX_ALIAS));
    }

    public void testSyncProfile() {
        final GrantApiKeyRequest.Grant grant = new GrantApiKeyRequest.Grant();
        grant.setType("password");
        grant.setUsername(RAC_USER_NAME);
        grant.setPassword(new SecureString(TEST_PASSWORD.toCharArray()));
        final SyncProfileRequest syncProfileRequest = new SyncProfileRequest(grant);
        client().execute(SyncProfileAction.INSTANCE, syncProfileRequest).actionGet();
    }
}
