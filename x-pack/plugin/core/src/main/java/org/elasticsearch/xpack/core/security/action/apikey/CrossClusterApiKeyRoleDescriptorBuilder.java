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

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class CrossClusterApiKeyRoleDescriptorBuilder {

    public static final String[] CCS_CLUSTER_PRIVILEGE_NAMES = { "cross_cluster_search" };
    public static final String[] CCR_CLUSTER_PRIVILEGE_NAMES = { "cross_cluster_replication" };
    public static final String[] CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES = { "cross_cluster_search", "cross_cluster_replication" };
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
        return CrossClusterApiKeyRoleDescriptorBuilder.PARSER.parse(
            JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, access),
            null
        );
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
        final String[] clusterPrivileges = roleDescriptor.getClusterPrivileges();
        if (false == Arrays.equals(clusterPrivileges, CCS_CLUSTER_PRIVILEGE_NAMES)
            && false == Arrays.equals(clusterPrivileges, CCR_CLUSTER_PRIVILEGE_NAMES)
            && false == Arrays.equals(clusterPrivileges, CCS_AND_CCR_CLUSTER_PRIVILEGE_NAMES)) {
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
    }
}
