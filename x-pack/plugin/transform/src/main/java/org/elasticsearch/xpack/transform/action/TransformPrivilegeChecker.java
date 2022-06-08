/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.transform.transforms.NullRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * {@link TransformPrivilegeChecker} is responsible for checking whether the user has the right privileges in order to work with transform.
 */
final class TransformPrivilegeChecker {

    static void checkPrivileges(
        String operationName,
        SecurityContext securityContext,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        Client client,
        TransformConfig config,
        boolean checkDestIndexPrivileges,
        ActionListener<Void> listener
    ) {
        final String username = securityContext.getUser().principal();

        ActionListener<HasPrivilegesResponse> hasPrivilegesResponseListener = ActionListener.wrap(
            response -> handlePrivilegesResponse(operationName, username, config.getId(), response, listener),
            listener::onFailure
        );

        HasPrivilegesRequest hasPrivilegesRequest = buildPrivilegesRequest(
            config,
            indexNameExpressionResolver,
            clusterState,
            username,
            checkDestIndexPrivileges
        );
        client.execute(HasPrivilegesAction.INSTANCE, hasPrivilegesRequest, hasPrivilegesResponseListener);
    }

    private static HasPrivilegesRequest buildPrivilegesRequest(
        TransformConfig config,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        String username,
        boolean checkDestIndexPrivileges
    ) {
        List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>(2);

        RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
            .indices(config.getSource().getIndex())
            // We need to read the source indices mapping to deduce the destination mapping, hence the need for view_index_metadata
            .privileges("read", "view_index_metadata")
            .build();
        indicesPrivileges.add(sourceIndexPrivileges);

        if (checkDestIndexPrivileges) {
            final String destIndex = config.getDestination().getIndex();
            final String[] concreteDest = indexNameExpressionResolver.concreteIndexNames(
                clusterState,
                IndicesOptions.lenientExpandOpen(),
                destIndex
            );

            List<String> destPrivileges = new ArrayList<>(4);
            destPrivileges.add("read");
            destPrivileges.add("index");
            // If the destination index does not exist, we can assume that we may have to create it on start.
            // We should check that the creating user has the privileges to create the index.
            if (concreteDest.length == 0) {
                destPrivileges.add("create_index");
            }
            if (config.getRetentionPolicyConfig() != null
                && config.getRetentionPolicyConfig() instanceof NullRetentionPolicyConfig == false) {
                destPrivileges.add("delete");
            }
            RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(destIndex)
                .privileges(destPrivileges)
                .build();
            indicesPrivileges.add(destIndexPrivileges);
        }

        HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
        privRequest.username(username);
        privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
        privRequest.indexPrivileges(indicesPrivileges.toArray(RoleDescriptor.IndicesPrivileges[]::new));
        return privRequest;
    }

    private static void handlePrivilegesResponse(
        String operationName,
        String username,
        String transformId,
        HasPrivilegesResponse privilegesResponse,
        ActionListener<Void> listener
    ) {
        if (privilegesResponse.isCompleteMatch()) {
            listener.onResponse(null);
        } else {
            List<String> missingPrivileges = privilegesResponse.getIndexPrivileges()
                .stream()
                .map(
                    indexPrivileges -> indexPrivileges.getPrivileges()
                        .entrySet()
                        .stream()
                        .filter(e -> Boolean.TRUE.equals(e.getValue()) == false)
                        .map(e -> e.getKey())
                        .collect(joining(", ", indexPrivileges.getResource() + ":[", "]"))
                )
                .collect(toList());
            listener.onFailure(
                Exceptions.authorizationError(
                    "Cannot {} transform [{}] because user {} lacks the required permissions {}",
                    operationName,
                    transformId,
                    username,
                    missingPrivileges
                )
            );
        }
    }

    private TransformPrivilegeChecker() {}
}
