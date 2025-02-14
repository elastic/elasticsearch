/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissionGroup;
import org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.generateRandomStringArray;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomArray;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomNonEmptySubsetOf;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_CLUSTER_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCR_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_CLUSTER_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES;
import static org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder.ROLE_DESCRIPTOR_NAME;

public final class RoleDescriptorTestHelper {

    public static Builder builder() {
        return new Builder();
    }

    public static RoleDescriptor randomRoleDescriptor() {
        return builder().allowReservedMetadata(randomBoolean())
            .allowRemoteIndices(randomBoolean())
            .allowRestriction(randomBoolean())
            .allowDescription(randomBoolean())
            .allowRemoteClusters(randomBoolean())
            .allowConfigurableClusterPrivileges(randomBoolean())
            .build();
    }

    public static Map<String, Object> randomRoleDescriptorMetadata(boolean allowReservedMetadata) {
        final Map<String, Object> metadata = new HashMap<>();
        while (randomBoolean()) {
            String key = randomAlphaOfLengthBetween(4, 12);
            if (allowReservedMetadata && randomBoolean()) {
                key = MetadataUtils.RESERVED_PREFIX + key;
            }
            final Object value = randomBoolean() ? randomInt() : randomAlphaOfLengthBetween(3, 50);
            metadata.put(key, value);
        }
        return metadata;
    }

    public static ConfigurableClusterPrivilege[] randomClusterPrivileges() {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> new ConfigurableClusterPrivilege[0];
            case 1 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 2 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 3 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ),
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 4 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ),
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 5 -> randomManageRolesPrivileges();
            default -> throw new IllegalStateException("Unexpected value");
        };
    }

    public static RoleDescriptor.ApplicationResourcePrivileges[] randomApplicationPrivileges() {
        final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges =
            new RoleDescriptor.ApplicationResourcePrivileges[randomIntBetween(0, 2)];
        for (int i = 0; i < applicationPrivileges.length; i++) {
            final RoleDescriptor.ApplicationResourcePrivileges.Builder builder = RoleDescriptor.ApplicationResourcePrivileges.builder();
            builder.application("app" + randomAlphaOfLengthBetween(5, 12) + (randomBoolean() ? "*" : ""));
            if (randomBoolean()) {
                builder.privileges("*");
            } else {
                builder.privileges(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
            }
            if (randomBoolean()) {
                builder.resources("*");
            } else {
                builder.resources(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
            }
            applicationPrivileges[i] = builder.build();
        }
        return applicationPrivileges;
    }

    public static ConfigurableClusterPrivilege[] randomManageRolesPrivileges() {
        List<ConfigurableClusterPrivileges.ManageRolesPrivilege.ManageRolesIndexPermissionGroup> indexPatternPrivileges = randomList(
            1,
            10,
            () -> {
                String[] indexPatterns = randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(5, 100));

                int startIndex = randomIntBetween(0, IndexPrivilege.names().size() - 2);
                int endIndex = randomIntBetween(startIndex + 1, IndexPrivilege.names().size());

                String[] indexPrivileges = IndexPrivilege.names().stream().toList().subList(startIndex, endIndex).toArray(String[]::new);
                return new ConfigurableClusterPrivileges.ManageRolesPrivilege.ManageRolesIndexPermissionGroup(
                    indexPatterns,
                    indexPrivileges
                );
            }
        );

        return new ConfigurableClusterPrivilege[] { new ConfigurableClusterPrivileges.ManageRolesPrivilege(indexPatternPrivileges) };
    }

    public static RoleDescriptor.RemoteIndicesPrivileges[] randomRemoteIndicesPrivileges(int min, int max) {
        return randomRemoteIndicesPrivileges(min, max, Set.of());
    }

    public static RoleDescriptor.RemoteIndicesPrivileges[] randomRemoteIndicesPrivileges(int min, int max, Set<String> excludedPrivileges) {
        final RoleDescriptor.IndicesPrivileges[] innerIndexPrivileges = randomIndicesPrivileges(min, max, excludedPrivileges);
        final RoleDescriptor.RemoteIndicesPrivileges[] remoteIndexPrivileges =
            new RoleDescriptor.RemoteIndicesPrivileges[innerIndexPrivileges.length];
        for (int i = 0; i < remoteIndexPrivileges.length; i++) {
            remoteIndexPrivileges[i] = new RoleDescriptor.RemoteIndicesPrivileges(
                innerIndexPrivileges[i],
                generateRandomStringArray(5, randomIntBetween(3, 9), false, false)
            );
        }
        return remoteIndexPrivileges;
    }

    public static RoleDescriptor.IndicesPrivileges[] randomIndicesPrivileges(int min, int max) {
        return randomIndicesPrivileges(min, max, Set.of());
    }

    public static RoleDescriptor.IndicesPrivileges[] randomIndicesPrivileges(int min, int max, Set<String> excludedPrivileges) {
        final RoleDescriptor.IndicesPrivileges[] indexPrivileges = new RoleDescriptor.IndicesPrivileges[randomIntBetween(min, max)];
        for (int i = 0; i < indexPrivileges.length; i++) {
            indexPrivileges[i] = randomIndicesPrivilegesBuilder(excludedPrivileges).build();
        }
        return indexPrivileges;
    }

    public static RoleDescriptor.IndicesPrivileges.Builder randomIndicesPrivilegesBuilder() {
        return randomIndicesPrivilegesBuilder(Set.of());
    }

    private static RoleDescriptor.IndicesPrivileges.Builder randomIndicesPrivilegesBuilder(Set<String> excludedPrivileges) {
        final Set<String> candidatePrivilegesNames = Sets.difference(IndexPrivilege.names(), excludedPrivileges);
        assert false == candidatePrivilegesNames.isEmpty() : "no candidate privilege names to random from";
        final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
            .privileges(randomSubsetOf(randomIntBetween(1, 4), candidatePrivilegesNames))
            .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
            .allowRestrictedIndices(randomBoolean());
        randomDlsFls(builder);
        return builder;
    }

    private static void randomDlsFls(RoleDescriptor.IndicesPrivileges.Builder builder) {
        if (randomBoolean()) {
            builder.query(randomBoolean() ? Strings.format("""
                { "term": { "%s" : "%s" } }
                """, randomAlphaOfLengthBetween(3, 24), randomAlphaOfLengthBetween(3, 24)) : """
                { "match_all": {} }
                """);
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.grantedFields("*");
                builder.deniedFields(generateRandomStringArray(4, randomIntBetween(4, 9), false, false));
            } else {
                builder.grantedFields(generateRandomStringArray(4, randomIntBetween(4, 9), false, false));
            }
        }
    }

    public static RoleDescriptor randomCrossClusterAccessRoleDescriptor() {
        final int searchSize = randomIntBetween(0, 3);
        final int replicationSize = randomIntBetween(searchSize == 0 ? 1 : 0, 3);
        assert searchSize + replicationSize > 0;

        final String[] clusterPrivileges;
        if (searchSize > 0 && replicationSize > 0) {
            clusterPrivileges = CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES;
        } else if (searchSize > 0) {
            clusterPrivileges = CCS_CLUSTER_PRIVILEGE_NAMES;
        } else {
            clusterPrivileges = CCR_CLUSTER_PRIVILEGE_NAMES;
        }

        final List<RoleDescriptor.IndicesPrivileges> indexPrivileges = new ArrayList<>();
        for (int i = 0; i < searchSize; i++) {
            final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(CCS_INDICES_PRIVILEGE_NAMES)
                .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
                .allowRestrictedIndices(randomBoolean());
            if (replicationSize == 0) {
                randomDlsFls(builder);
            }
            indexPrivileges.add(builder.build());
        }
        for (int i = 0; i < replicationSize; i++) {
            final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(CCR_INDICES_PRIVILEGE_NAMES)
                .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
                .allowRestrictedIndices(randomBoolean());
            indexPrivileges.add(builder.build());
        }

        return new RoleDescriptor(
            ROLE_DESCRIPTOR_NAME,
            clusterPrivileges,
            indexPrivileges.toArray(RoleDescriptor.IndicesPrivileges[]::new),
            null
        );
    }

    public static List<RoleDescriptor> randomUniquelyNamedRoleDescriptors(int minSize, int maxSize) {
        return randomValueOtherThanMany(
            roleDescriptors -> roleDescriptors.stream().map(RoleDescriptor::getName).distinct().count() != roleDescriptors.size(),
            () -> randomList(minSize, maxSize, () -> builder().build())
        );
    }

    public static RemoteClusterPermissions randomRemoteClusterPermissions(int maxGroups) {
        final RemoteClusterPermissions remoteClusterPermissions = new RemoteClusterPermissions();
        final String[] supportedPermissions = RemoteClusterPermissions.getSupportedRemoteClusterPermissions().toArray(new String[0]);
        for (int i = 0; i < maxGroups; i++) {
            remoteClusterPermissions.addGroup(
                new RemoteClusterPermissionGroup(
                    randomNonEmptySubsetOf(Arrays.asList(supportedPermissions)).toArray(new String[0]),
                    generateRandomStringArray(5, randomIntBetween(3, 9), false, false)
                )
            );
        }
        return remoteClusterPermissions;
    }

    public static class Builder {

        private boolean allowReservedMetadata = false;
        private boolean allowRemoteIndices = false;
        private boolean alwaysIncludeRemoteIndices = false;
        private boolean allowRestriction = false;
        private boolean allowDescription = false;
        private boolean allowRemoteClusters = false;
        private boolean allowConfigurableClusterPrivileges = false;

        public Builder() {}

        public Builder allowReservedMetadata(boolean allowReservedMetadata) {
            this.allowReservedMetadata = allowReservedMetadata;
            return this;
        }

        public Builder allowConfigurableClusterPrivileges(boolean allowConfigurableClusterPrivileges) {
            this.allowConfigurableClusterPrivileges = allowConfigurableClusterPrivileges;
            return this;
        }

        public Builder alwaysIncludeRemoteIndices() {
            this.alwaysIncludeRemoteIndices = true;
            return this;
        }

        public Builder allowRemoteIndices(boolean allowRemoteIndices) {
            this.allowRemoteIndices = allowRemoteIndices;
            return this;
        }

        public Builder allowRestriction(boolean allowRestriction) {
            this.allowRestriction = allowRestriction;
            return this;
        }

        public Builder allowDescription(boolean allowDescription) {
            this.allowDescription = allowDescription;
            return this;
        }

        public Builder allowRemoteClusters(boolean allowRemoteClusters) {
            this.allowRemoteClusters = allowRemoteClusters;
            return this;
        }

        public RoleDescriptor build() {
            final RoleDescriptor.RemoteIndicesPrivileges[] remoteIndexPrivileges;
            if (alwaysIncludeRemoteIndices || (allowRemoteIndices && randomBoolean())) {
                remoteIndexPrivileges = randomRemoteIndicesPrivileges(0, 3);
            } else {
                remoteIndexPrivileges = null;
            }

            RemoteClusterPermissions remoteClusters = RemoteClusterPermissions.NONE;
            if (allowRemoteClusters && randomBoolean()) {
                remoteClusters = randomRemoteClusterPermissions(randomIntBetween(1, 5));
            }

            return new RoleDescriptor(
                randomAlphaOfLengthBetween(3, 90),
                randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
                randomIndicesPrivileges(0, 3),
                randomApplicationPrivileges(),
                allowConfigurableClusterPrivileges ? randomClusterPrivileges() : null,
                generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
                randomRoleDescriptorMetadata(allowReservedMetadata),
                Map.of(),
                remoteIndexPrivileges,
                remoteClusters,
                allowRestriction ? RoleRestrictionTests.randomWorkflowsRestriction(1, 3) : null,
                allowDescription ? randomAlphaOfLengthBetween(0, 20) : null
            );
        }
    }
}
