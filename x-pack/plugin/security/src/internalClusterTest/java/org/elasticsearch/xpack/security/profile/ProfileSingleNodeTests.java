/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class ProfileSingleNodeTests extends SecuritySingleNodeTestCase {

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

    public void testProfileIndexAutoCreation() {
        var indexResponse = client().prepareIndex(
            randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_PROFILE_INDEX_8, RestrictedIndicesNames.SECURITY_PROFILE_ALIAS)
        ).setSource(Map.of("uid", randomAlphaOfLength(22))).get();

        assertThat(indexResponse.status().getStatus(), equalTo(201));

        var getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(RestrictedIndicesNames.INTERNAL_SECURITY_PROFILE_INDEX_8);

        var getIndexResponse = client().execute(GetIndexAction.INSTANCE, getIndexRequest).actionGet();

        assertThat(getIndexResponse.getIndices(), arrayContaining(RestrictedIndicesNames.INTERNAL_SECURITY_PROFILE_INDEX_8));

        var aliases = getIndexResponse.getAliases().get(RestrictedIndicesNames.INTERNAL_SECURITY_PROFILE_INDEX_8);
        assertThat(aliases, hasSize(1));
        assertThat(aliases.get(0).alias(), equalTo(RestrictedIndicesNames.SECURITY_PROFILE_ALIAS));

        final Settings settings = getIndexResponse.getSettings().get(RestrictedIndicesNames.INTERNAL_SECURITY_PROFILE_INDEX_8);
        assertThat(settings.get("index.number_of_shards"), equalTo("1"));
        assertThat(settings.get("index.auto_expand_replicas"), equalTo("0-1"));
        assertThat(settings.get("index.routing.allocation.include._tier_preference"), equalTo("data_content"));

        final Map<String, Object> mappings = getIndexResponse.getMappings()
            .get(RestrictedIndicesNames.INTERNAL_SECURITY_PROFILE_INDEX_8)
            .getSourceAsMap();

        @SuppressWarnings("unchecked")
        final Set<String> topLevelFields = ((Map<String, Object>) mappings.get("properties")).keySet();
        assertThat(topLevelFields, hasItems("uid", "enabled", "last_synchronized", "redirect_uid", "user", "access", "application_data"));
    }
}
