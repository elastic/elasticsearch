/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SecurityMigrationsTests extends ESTestCase {
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
    }

    private static ExpressionRoleMapping reservedRoleMapping(String name) {
        return new ExpressionRoleMapping(
            name + SecurityMigrations.CleanupRoleMappingDuplicatesMigration.RESERVED_ROLE_MAPPING_SUFFIX,
            null,
            null,
            null,
            Map.of(SecurityMigrations.CleanupRoleMappingDuplicatesMigration.METADATA_READ_ONLY_FLAG_KEY, true),
            true
        );
    }

    private static ExpressionRoleMapping nativeRoleMapping(String name) {
        return new ExpressionRoleMapping(name, null, null, null, null, true);
    }

}
