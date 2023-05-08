/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.ApplicationPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteIndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;

import java.util.List;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class StorageInternalUserTests extends ESTestCase {

    public void testRoleDescriptor() {
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        final SimpleRole role = Role.buildFromRoleDescriptor(
            StorageInternalUser.ROLE_DESCRIPTOR,
            fieldPermissionsCache,
            TestRestrictedIndices.RESTRICTED_INDICES
        );

        assertThat(role.cluster().privileges(), hasSize(0));
        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.application(), is(ApplicationPermission.NONE));
        assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));

        final List<String> sampleAllowedActions = List.of(RefreshAction.NAME, TransportUnpromotableShardRefreshAction.NAME);
        checkIndexAccess(role, randomFrom(sampleAllowedActions), randomAlphaOfLengthBetween(4, 8), true);
        checkIndexAccess(role, randomFrom(sampleAllowedActions), ".ds-" + randomAlphaOfLengthBetween(4, 8), true);
        checkIndexAccess(role, randomFrom(sampleAllowedActions), INTERNAL_SECURITY_MAIN_INDEX_7, true);

        final List<String> sampleDeniedActions = List.of(GetAction.NAME, BulkAction.NAME, PutMappingAction.NAME, DeleteIndexAction.NAME);
        checkIndexAccess(role, randomFrom(sampleDeniedActions), randomAlphaOfLengthBetween(4, 8), false);
        checkIndexAccess(role, randomFrom(sampleDeniedActions), ".ds-" + randomAlphaOfLengthBetween(4, 8), false);
        checkIndexAccess(role, randomFrom(sampleDeniedActions), INTERNAL_SECURITY_MAIN_INDEX_7, false);
    }

    private static void checkIndexAccess(SimpleRole role, String action, String indexName, boolean expectedValue) {
        assertThat("Role " + role + " should grant " + action, role.indices().check(action), is(expectedValue));

        final Automaton automaton = role.indices().allowedActionsMatcher(indexName);
        assertThat(
            "Role " + role + " should grant " + action + " access to " + indexName,
            new CharacterRunAutomaton(automaton).run(action),
            is(expectedValue)
        );

        final IndexMetadata metadata = IndexMetadata.builder(indexName).settings(indexSettings(Version.CURRENT, 1, 1)).build();
        final IndexAbstraction.ConcreteIndex index = new IndexAbstraction.ConcreteIndex(metadata);
        assertThat(
            "Role " + role + " should grant " + action + " access to " + indexName,
            role.allowedIndicesMatcher(action).test(index),
            is(expectedValue)
        );
    }

}
