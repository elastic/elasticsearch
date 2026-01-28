/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class CrossClusterApiKeyRoleDescriptorBuilder {

    // monitor_enrich is needed for ES|QL + ENRICH and https://github.com/elastic/elasticsearch/issues/106926 is related
    public static final String[] CCS_CLUSTER_PRIVILEGE_NAMES = { "cross_cluster_search", "monitor_enrich" };
    public static final String[] CCR_CLUSTER_PRIVILEGE_NAMES = { "cross_cluster_replication" };
    public static final String[] CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES = Stream.concat(
        Arrays.stream(CCS_CLUSTER_PRIVILEGE_NAMES),
        Arrays.stream(CCR_CLUSTER_PRIVILEGE_NAMES)
    ).toArray(String[]::new);
    public static final String[] CCS_INDICES_PRIVILEGE_NAMES = { "read", "read_cross_cluster", "view_index_metadata" };
    public static final String[] CCR_INDICES_PRIVILEGE_NAMES = { "cross_cluster_replication", "cross_cluster_replication_internal" };
    public static final String ROLE_DESCRIPTOR_NAME = "cross_cluster";

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<CrossClusterApiKeyRoleDescriptorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "cross_cluster_api_key_request_access",
        false,
        (args, v) -> new CrossClusterApiKeyRoleDescriptorBuilder(
            (List<RoleDescriptor.IndicesPrivileges>) args[0],
            (List<RoleDescriptor.IndicesPrivileges>) args[1]
        )
    );

    static {
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptor.parseIndexWithPredefinedPrivileges(ROLE_DESCRIPTOR_NAME, CCS_INDICES_PRIVILEGE_NAMES, p),
            new ParseField("search")
        );
        PARSER.declareObjectArray(
            optionalConstructorArg(),
            (p, c) -> RoleDescriptor.parseIndexWithPredefinedPrivileges(ROLE_DESCRIPTOR_NAME, CCR_INDICES_PRIVILEGE_NAMES, p),
            new ParseField("replication")
        );
    }

    private final List<RoleDescriptor.IndicesPrivileges> search;
    private final List<RoleDescriptor.IndicesPrivileges> replication;

    private CrossClusterApiKeyRoleDescriptorBuilder(
        List<RoleDescriptor.IndicesPrivileges> search,
        List<RoleDescriptor.IndicesPrivileges> replication
    ) {
        this.search = search == null ? List.of() : search;
        this.replication = replication == null ? List.of() : replication;
        assert this.search.stream().allMatch(p -> Arrays.equals(p.getPrivileges(), CCS_INDICES_PRIVILEGE_NAMES));
        assert this.replication.stream().allMatch(p -> Arrays.equals(p.getPrivileges(), CCR_INDICES_PRIVILEGE_NAMES));
    }

    public RoleDescriptor build() {
        final String[] clusterPrivileges;
        if (search.isEmpty() && replication.isEmpty()) {
            throw new IllegalArgumentException("must specify non-empty access for either [search] or [replication]");
        } else if (search.isEmpty()) {
            clusterPrivileges = CCR_CLUSTER_PRIVILEGE_NAMES;
        } else if (replication.isEmpty()) {
            clusterPrivileges = CCS_CLUSTER_PRIVILEGE_NAMES;
        } else {
            clusterPrivileges = CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES;
            if (search.stream().anyMatch(RoleDescriptor.IndicesPrivileges::isUsingDocumentOrFieldLevelSecurity)) {
                throw new IllegalArgumentException("search does not support document or field level security if replication is assigned");
            }
        }

        if (replication.stream().anyMatch(RoleDescriptor.IndicesPrivileges::isUsingDocumentOrFieldLevelSecurity)) {
            throw new IllegalArgumentException("replication does not support document or field level security");
        }

        return new RoleDescriptor(
            ROLE_DESCRIPTOR_NAME,
            clusterPrivileges,
            CollectionUtils.concatLists(search, replication).toArray(RoleDescriptor.IndicesPrivileges[]::new),
            null
        );
    }

    public static CrossClusterApiKeyRoleDescriptorBuilder parse(String access) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, access)) {
            return CrossClusterApiKeyRoleDescriptorBuilder.PARSER.parse(parser, null);
        }
    }

    static void validate(RoleDescriptor roleDescriptor) {
        if (false == ROLE_DESCRIPTOR_NAME.equals(roleDescriptor.getName())) {
            throw new IllegalArgumentException("invalid role descriptor name [" + roleDescriptor.getName() + "]");
        }
        if (roleDescriptor.hasApplicationPrivileges()) {
            throw new IllegalArgumentException("application privilege must be empty");
        }
        if (roleDescriptor.hasRunAs()) {
            throw new IllegalArgumentException("run_as privilege must be empty");
        }
        if (roleDescriptor.hasConfigurableClusterPrivileges()) {
            throw new IllegalArgumentException("configurable cluster privilege must be empty");
        }
        if (roleDescriptor.hasRemoteIndicesPrivileges()) {
            throw new IllegalArgumentException("remote indices privileges must be empty");
        }
        if (roleDescriptor.hasRemoteClusterPermissions()) {
            throw new IllegalArgumentException("remote cluster permissions must be empty");
        }
        final String[] clusterPrivileges = roleDescriptor.getClusterPrivileges();
        // must contain either "cross_cluster_search" or "cross_cluster_replication" or both
        if ((Arrays.asList(clusterPrivileges).contains("cross_cluster_search")
            || Arrays.asList(clusterPrivileges).contains("cross_cluster_replication")) == false) {
            throw new IllegalArgumentException(
                "invalid cluster privileges: [" + Strings.arrayToCommaDelimitedString(clusterPrivileges) + "]"
            );
        }
        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = roleDescriptor.getIndicesPrivileges();
        if (indicesPrivileges.length == 0) {
            throw new IllegalArgumentException("indices privileges must not be empty");
        }

        for (RoleDescriptor.IndicesPrivileges indexPrivilege : indicesPrivileges) {
            final String[] privileges = indexPrivilege.getPrivileges();
            if (Arrays.equals(privileges, CCR_INDICES_PRIVILEGE_NAMES)) {
                if (indexPrivilege.isUsingDocumentOrFieldLevelSecurity()) {
                    throw new IllegalArgumentException("replication does not support document or field level security");
                }
            } else if (false == Arrays.equals(privileges, CCS_INDICES_PRIVILEGE_NAMES)) {
                throw new IllegalArgumentException("invalid indices privileges: [" + Strings.arrayToCommaDelimitedString(privileges));
            }
        }
        // Note: we are skipping the check for document or field level security on search (with replication) here, since validate is called
        // for instance as part of the Get and Query APIs, which need to continue to handle legacy role descriptors.
    }

    /**
     * Pre-GA versions of RCS 2.0 (8.13-) allowed users to use DLS/FLS for "search" when both "search" and "replication" are both defined.
     * Post-GA versions of RCS 2.0 (8.14+) allow users to use DLS/FLS only when "search" is defined. Defining DLS/FLS when both "search"
     * and "replication" are defined in not allowed. Legacy here is in reference to pre-GA CCx API keys. This method should only be
     * called to check the fulfilling cluster's API key role descriptor.
     */
    public static void checkForInvalidLegacyRoleDescriptors(String apiKeyId, List<RoleDescriptor> roleDescriptors) {
        assert roleDescriptors.size() == 1;
        final var roleDescriptor = roleDescriptors.get(0);
        final String[] clusterPrivileges = roleDescriptor.getClusterPrivileges();
        // only need to check if both "search" and "replication" are defined
        // no need to check for DLS if set of cluster privileges are not the set used pre 8.14
        final String[] legacyClusterPrivileges = { "cross_cluster_search", "cross_cluster_replication" };
        final boolean hasBoth = Arrays.equals(clusterPrivileges, legacyClusterPrivileges);
        if (false == hasBoth) {
            return;
        }

        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = roleDescriptor.getIndicesPrivileges();
        for (RoleDescriptor.IndicesPrivileges indexPrivilege : indicesPrivileges) {
            final String[] privileges = indexPrivilege.getPrivileges();
            final String[] legacyIndicesPrivileges = { "read", "read_cross_cluster", "view_index_metadata" };
            // find the "search" privilege, no need to check for DLS if set of index privileges are not the set used pre 8.14
            if (Arrays.equals(privileges, legacyIndicesPrivileges)) {
                if (indexPrivilege.isUsingDocumentOrFieldLevelSecurity()) {
                    throw new IllegalArgumentException(
                        "Cross cluster API key ["
                            + apiKeyId
                            + "] is invalid: search does not support document or field level security if replication is assigned"
                    );
                }
            }
        }
    }
}
