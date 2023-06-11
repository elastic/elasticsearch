/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ApplicationPermission;
import org.elasticsearch.xpack.core.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteIndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.core.security.authz.permission.SimpleRole;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;

import java.util.List;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_TOKENS_INDEX_7;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.SECURITY_TOKENS_ALIAS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class InternalUsersTests extends ESTestCase {

    public void testSystemUser() {
        assertThat(InternalUsers.getUser("_system"), is(InternalUsers.SYSTEM_USER));
    }

    public void testXPackUser() {
        assertThat(InternalUsers.getUser("_xpack"), is(InternalUsers.XPACK_USER));

        final SimpleRole role = getLocalClusterRole(InternalUsers.XPACK_USER);

        assertThat(role.runAs().toString(), Operations.isTotal(role.runAs().getPrivilege().getAutomaton()), is(true));
        assertThat(role.application(), is(ApplicationPermission.NONE));
        assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));

        final List<String> sampleClusterActions = List.of(
            ClusterStateAction.NAME,
            PutComponentTemplateAction.NAME,
            DeleteStoredScriptAction.NAME,
            UpdateJobAction.NAME,
            CleanupRepositoryAction.NAME
        );
        checkClusterAccess(InternalUsers.XPACK_USER, role, randomFrom(sampleClusterActions), true);

        final List<String> sampleIndexActions = List.of(
            GetAction.NAME,
            BulkAction.NAME,
            RefreshAction.NAME,
            CreateIndexAction.NAME,
            PutMappingAction.NAME,
            DeleteIndexAction.NAME
        );
        checkIndexAccess(role, randomFrom(sampleIndexActions), randomAlphaOfLengthBetween(3, 12), true);
        checkIndexAccess(
            role,
            randomFrom(sampleIndexActions),
            randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7, SECURITY_TOKENS_ALIAS, INTERNAL_SECURITY_TOKENS_INDEX_7),
            false
        );
    }

    public void testXPackSecurityUser() {
        assertThat(InternalUsers.getUser("_xpack_security"), is(InternalUsers.XPACK_SECURITY_USER));

        final SimpleRole role = getLocalClusterRole(InternalUsers.XPACK_SECURITY_USER);

        assertThat(role.runAs().toString(), Operations.isTotal(role.runAs().getPrivilege().getAutomaton()), is(true));
        assertThat(role.application(), is(ApplicationPermission.NONE));
        assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));

        final List<String> sampleClusterActions = List.of(
            ClusterStateAction.NAME,
            PutComponentTemplateAction.NAME,
            DeleteStoredScriptAction.NAME,
            UpdateJobAction.NAME,
            CleanupRepositoryAction.NAME
        );
        checkClusterAccess(InternalUsers.XPACK_SECURITY_USER, role, randomFrom(sampleClusterActions), true);

        final List<String> sampleIndexActions = List.of(
            GetAction.NAME,
            BulkAction.NAME,
            RefreshAction.NAME,
            CreateIndexAction.NAME,
            PutMappingAction.NAME,
            DeleteIndexAction.NAME
        );
        checkIndexAccess(
            role,
            randomFrom(sampleIndexActions),
            randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7, SECURITY_TOKENS_ALIAS, INTERNAL_SECURITY_TOKENS_INDEX_7),
            true
        );
        checkIndexAccess(role, randomFrom(sampleIndexActions), randomAlphaOfLengthBetween(3, 12), true);
    }

    public void testSecurityProfileUser() {
        assertThat(InternalUsers.getUser("_security_profile"), is(InternalUsers.SECURITY_PROFILE_USER));

        final SimpleRole role = getLocalClusterRole(InternalUsers.SECURITY_PROFILE_USER);

        assertThat(role.cluster().privileges(), hasSize(0));
        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.application(), is(ApplicationPermission.NONE));
        assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));

        final List<String> sampleAllowedActions = List.of(
            GetAction.NAME,
            BulkAction.NAME,
            RefreshAction.NAME,
            CreateIndexAction.NAME,
            PutMappingAction.NAME,
            DeleteIndexAction.NAME
        );
        checkIndexAccess(role, randomFrom(sampleAllowedActions), ".security-profile", true);
        checkIndexAccess(role, randomFrom(sampleAllowedActions), ".security-profile-" + randomIntBetween(1, 9), true);
        checkIndexAccess(
            role,
            randomFrom(sampleAllowedActions),
            randomFrom(SECURITY_MAIN_ALIAS, INTERNAL_SECURITY_MAIN_INDEX_7, SECURITY_TOKENS_ALIAS, INTERNAL_SECURITY_TOKENS_INDEX_7),
            false
        );
        checkIndexAccess(role, randomFrom(sampleAllowedActions), randomAlphaOfLengthBetween(3, 12), false);
    }

    public void testAsyncSearchUser() {
        assertThat(InternalUsers.getUser("_async_search"), is(InternalUsers.ASYNC_SEARCH_USER));

        final SimpleRole role = getLocalClusterRole(InternalUsers.ASYNC_SEARCH_USER);

        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.application(), is(ApplicationPermission.NONE));
        assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));

        checkClusterAccess(InternalUsers.ASYNC_SEARCH_USER, role, CancelTasksAction.NAME, true);
        checkClusterAccess(InternalUsers.ASYNC_SEARCH_USER, role, ClusterStateAction.NAME, false);

        final List<String> sampleAllowedActions = List.of(
            GetAction.NAME,
            BulkAction.NAME,
            RefreshAction.NAME,
            CreateIndexAction.NAME,
            PutMappingAction.NAME,
            DeleteIndexAction.NAME
        );
        checkIndexAccess(role, randomFrom(sampleAllowedActions), XPackPlugin.ASYNC_RESULTS_INDEX, true);
        checkIndexAccess(
            role,
            randomFrom(sampleAllowedActions),
            XPackPlugin.ASYNC_RESULTS_INDEX + "-" + randomAlphaOfLengthBetween(4, 8),
            true
        );
        checkIndexAccess(role, randomFrom(sampleAllowedActions), randomAlphaOfLengthBetween(3, 12), false);
    }

    public void testStorageUser() {
        assertThat(InternalUsers.getUser("_storage"), is(InternalUsers.STORAGE_USER));

        final SimpleRole role = getLocalClusterRole(InternalUsers.STORAGE_USER);

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

    public void testDlmUser() {
        assertThat(InternalUsers.getUser("_dlm"), is(InternalUsers.DLM_USER));
        assertThat(
            InternalUsers.DLM_USER.getLocalClusterRoleDescriptor().get().getMetadata(),
            equalTo(MetadataUtils.DEFAULT_RESERVED_METADATA)
        );

        final SimpleRole role = getLocalClusterRole(InternalUsers.DLM_USER);

        assertThat(role.cluster(), is(ClusterPermission.NONE));
        assertThat(role.runAs(), is(RunAsPermission.NONE));
        assertThat(role.application(), is(ApplicationPermission.NONE));
        assertThat(role.remoteIndices(), is(RemoteIndicesPermission.NONE));

        final String allowedSystemDataStream = ".fleet-actions-results";
        for (var group : role.indices().groups()) {
            if (group.allowRestrictedIndices()) {
                assertThat(group.indices(), arrayContaining(allowedSystemDataStream));
            }
        }

        final List<String> sampleIndexActions = List.of(
            RolloverAction.NAME,
            DeleteIndexAction.NAME,
            ForceMergeAction.NAME,
            IndicesStatsAction.NAME
        );
        final String dataStream = randomAlphaOfLengthBetween(3, 12);
        checkIndexAccess(role, randomFrom(sampleIndexActions), dataStream, true);
        // Also check backing index access
        checkIndexAccess(
            role,
            randomFrom(sampleIndexActions),
            DataStream.BACKING_INDEX_PREFIX + dataStream + randomAlphaOfLengthBetween(4, 8),
            true
        );

        checkIndexAccess(role, randomFrom(sampleIndexActions), allowedSystemDataStream, true);
        checkIndexAccess(
            role,
            randomFrom(sampleIndexActions),
            DataStream.BACKING_INDEX_PREFIX + allowedSystemDataStream + randomAlphaOfLengthBetween(4, 8),
            true
        );

        checkIndexAccess(role, randomFrom(sampleIndexActions), randomFrom(TestRestrictedIndices.SAMPLE_RESTRICTED_NAMES), false);
    }

    public void testRegularUser() {
        var username = randomAlphaOfLengthBetween(4, 12);
        expectThrows(IllegalStateException.class, () -> InternalUsers.getUser(username));
    }

    private static SimpleRole getLocalClusterRole(InternalUser internalUser) {
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        return Role.buildFromRoleDescriptor(
            internalUser.getLocalClusterRoleDescriptor().get(),
            fieldPermissionsCache,
            TestRestrictedIndices.RESTRICTED_INDICES
        );
    }

    private static void checkClusterAccess(InternalUser user, SimpleRole role, String action, boolean expectedValue) {
        Authentication authentication = AuthenticationTestHelper.builder().internal(user).build();
        assertThat(
            "Role [" + role + "] for user [" + user + "] should grant " + action,
            role.cluster().check(action, mock(TransportRequest.class), authentication),
            is(expectedValue)
        );

    }

    private static void checkIndexAccess(SimpleRole role, String action, String indexName, boolean expectedValue) {
        if (expectedValue) {
            // Can't check this if "expectedValue" is false, because the role might grant the action for a different index
            assertThat("Role " + role + " should grant " + action, role.indices().check(action), is(true));
        }

        final Automaton automaton = role.indices().allowedActionsMatcher(indexName);
        assertThat(
            "Role " + role + ", action " + action + " access to " + indexName,
            new CharacterRunAutomaton(automaton).run(action),
            is(expectedValue)
        );

        final IndexMetadata metadata = IndexMetadata.builder(indexName).settings(indexSettings(Version.CURRENT, 1, 1)).build();
        final IndexAbstraction.ConcreteIndex index = new IndexAbstraction.ConcreteIndex(metadata);
        assertThat(
            "Role " + role + ", action " + action + " access to " + indexName,
            role.allowedIndicesMatcher(action).test(index),
            is(expectedValue)
        );
    }

}
