/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingResponse;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsAction;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequest;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsResponse;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityMigrationsTests extends ESTestCase {
    private ThreadPool threadPool;
    private Client client;

    public void testGetDuplicateRoleMappingNames() {
        assertThat(SecurityMigrations.CleanupRoleMappingDuplicatesMigration.getDuplicateRoleMappingNames(), empty());
        assertThat(
            SecurityMigrations.CleanupRoleMappingDuplicatesMigration.getDuplicateRoleMappingNames(
                nativeRoleMapping("roleMapping1"),
                nativeRoleMapping("roleMapping2")
            ),
            empty()
        );
        assertThat(
            SecurityMigrations.CleanupRoleMappingDuplicatesMigration.getDuplicateRoleMappingNames(
                nativeRoleMapping("roleMapping1"),
                reservedRoleMapping("roleMapping1")
            ),
            equalTo(List.of("roleMapping1"))
        );

        {
            List<String> duplicates = SecurityMigrations.CleanupRoleMappingDuplicatesMigration.getDuplicateRoleMappingNames(
                nativeRoleMapping("roleMapping1"),
                nativeRoleMapping("roleMapping2"),
                reservedRoleMapping("roleMapping1"),
                reservedRoleMapping("roleMapping2")
            );
            assertThat(duplicates, hasSize(2));
            assertThat(duplicates, containsInAnyOrder("roleMapping1", "roleMapping2"));
        }
        {
            List<String> duplicates = SecurityMigrations.CleanupRoleMappingDuplicatesMigration.getDuplicateRoleMappingNames(
                nativeRoleMapping("roleMapping1"),
                nativeRoleMapping("roleMapping2"),
                nativeRoleMapping("roleMapping3"),
                reservedRoleMapping("roleMapping1"),
                reservedRoleMapping("roleMapping2"),
                reservedRoleMapping("roleMapping4")
            );
            assertThat(duplicates, hasSize(2));
            assertThat(duplicates, containsInAnyOrder("roleMapping1", "roleMapping2"));
        }
        {
            List<String> duplicates = SecurityMigrations.CleanupRoleMappingDuplicatesMigration.getDuplicateRoleMappingNames(
                nativeRoleMapping("roleMapping1" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX),
                nativeRoleMapping("roleMapping2" + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX),
                nativeRoleMapping("roleMapping3"),
                reservedRoleMapping("roleMapping1"),
                reservedRoleMapping("roleMapping2"),
                reservedRoleMapping("roleMapping3")
            );
            assertThat(duplicates, hasSize(1));
            assertThat(duplicates, containsInAnyOrder("roleMapping3"));
        }
    }

    private static ExpressionRoleMapping reservedRoleMapping(String name) {
        return new ExpressionRoleMapping(
            name + ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_SUFFIX,
            null,
            null,
            null,
            Map.of(ExpressionRoleMapping.READ_ONLY_ROLE_MAPPING_METADATA_FLAG, true),
            true
        );
    }

    private static ExpressionRoleMapping nativeRoleMapping(String name) {
        return new ExpressionRoleMapping(name, null, null, null, randomBoolean() ? null : Map.of(), true);
    }

    public void testCleanupRoleMappingDuplicatesMigrationPartialFailure() {
        // Make sure migration continues even if a duplicate is not found
        SecurityIndexManager securityIndexManager = mock(SecurityIndexManager.class);
        SecurityIndexManager.IndexState projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndexManager.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.getRoleMappingsCleanupMigrationStatus()).thenReturn(
            SecurityIndexManager.RoleMappingsCleanupMigrationStatus.READY
        );
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<GetRoleMappingsResponse> listener = (ActionListener<GetRoleMappingsResponse>) args[2];
            listener.onResponse(
                new GetRoleMappingsResponse(
                    nativeRoleMapping("duplicate-0"),
                    reservedRoleMapping("duplicate-0"),
                    nativeRoleMapping("duplicate-1"),
                    reservedRoleMapping("duplicate-1"),
                    nativeRoleMapping("duplicate-2"),
                    reservedRoleMapping("duplicate-2")
                )
            );
            return null;
        }).when(client).execute(eq(GetRoleMappingsAction.INSTANCE), any(GetRoleMappingsRequest.class), any());

        final boolean[] duplicatesDeleted = new boolean[3];
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<DeleteRoleMappingResponse> listener = (ActionListener<DeleteRoleMappingResponse>) args[2];
            DeleteRoleMappingRequest request = (DeleteRoleMappingRequest) args[1];
            if (request.getName().equals("duplicate-0")) {
                duplicatesDeleted[0] = true;
            }
            if (request.getName().equals("duplicate-1")) {
                if (randomBoolean()) {
                    listener.onResponse(new DeleteRoleMappingResponse(false));
                } else {
                    listener.onFailure(new IllegalStateException("bad state"));
                }
            }
            if (request.getName().equals("duplicate-2")) {
                duplicatesDeleted[2] = true;
            }
            return null;
        }).when(client).execute(eq(DeleteRoleMappingAction.INSTANCE), any(DeleteRoleMappingRequest.class), any());

        SecurityMigrations.SecurityMigration securityMigration = new SecurityMigrations.CleanupRoleMappingDuplicatesMigration();
        securityMigration.migrate(securityIndexManager, client, ActionListener.noop());

        assertTrue(duplicatesDeleted[0]);
        assertFalse(duplicatesDeleted[1]);
        assertTrue(duplicatesDeleted[2]);
    }

    @Before
    public void createClientAndThreadPool() {
        threadPool = new TestThreadPool("cleanup role mappings test pool");
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

}
