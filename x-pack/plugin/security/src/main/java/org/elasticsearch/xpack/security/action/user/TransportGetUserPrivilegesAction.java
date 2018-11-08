/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;

/**
 * Transport action for {@link GetUserPrivilegesAction}
 */
public class TransportGetUserPrivilegesAction extends HandledTransportAction<GetUserPrivilegesRequest, GetUserPrivilegesResponse> {

    private final ThreadPool threadPool;
    private final AuthorizationService authorizationService;

    @Inject
    public TransportGetUserPrivilegesAction(ThreadPool threadPool, TransportService transportService,
                                            ActionFilters actionFilters, AuthorizationService authorizationService) {
        super(GetUserPrivilegesAction.NAME, transportService, actionFilters, GetUserPrivilegesRequest::new);
        this.threadPool = threadPool;
        this.authorizationService = authorizationService;
    }

    @Override
    protected void doExecute(Task task, GetUserPrivilegesRequest request, ActionListener<GetUserPrivilegesResponse> listener) {
        final String username = request.username();

        final User user = Authentication.getAuthentication(threadPool.getThreadContext()).getUser();
        if (user.principal().equals(username) == false) {
            listener.onFailure(new IllegalArgumentException("users may only list the privileges of their own account"));
            return;
        }

        authorizationService.roles(user, ActionListener.wrap(
            role -> listener.onResponse(buildResponseObject(role)),
            listener::onFailure));
    }

    // package protected for testing
    GetUserPrivilegesResponse buildResponseObject(Role userRole) {
        logger.trace(() -> new ParameterizedMessage("List privileges for role [{}]", arrayToCommaDelimitedString(userRole.names())));

        // We use sorted sets for Strings because they will typically be small, and having a predictable order allows for simpler testing
        final Set<String> cluster = new TreeSet<>();
        // But we don't have a meaningful ordering for objects like ConditionalClusterPrivilege, so the tests work with "random" ordering
        final Set<ConditionalClusterPrivilege> conditionalCluster = new HashSet<>();
        for (Tuple<ClusterPrivilege, ConditionalClusterPrivilege> tup : userRole.cluster().privileges()) {
            if (tup.v2() == null) {
                if (ClusterPrivilege.NONE.equals(tup.v1()) == false) {
                    cluster.addAll(tup.v1().name());
                }
            } else {
                conditionalCluster.add(tup.v2());
            }
        }

        final Set<GetUserPrivilegesResponse.Indices> indices = new LinkedHashSet<>();
        for (IndicesPermission.Group group : userRole.indices()) {
            final Set<BytesReference> queries = group.getQuery() == null ? Collections.emptySet() : group.getQuery();
            final Set<FieldPermissionsDefinition.FieldGrantExcludeGroup> fieldSecurity = group.getFieldPermissions().hasFieldLevelSecurity()
                ? group.getFieldPermissions().getFieldPermissionsDefinition().getFieldGrantExcludeGroups() : Collections.emptySet();
            indices.add(new GetUserPrivilegesResponse.Indices(
                Arrays.asList(group.indices()),
                group.privilege().name(),
                fieldSecurity,
                queries
            ));
        }

        final Set<RoleDescriptor.ApplicationResourcePrivileges> application = new LinkedHashSet<>();
        for (String applicationName : userRole.application().getApplicationNames()) {
            for (ApplicationPrivilege privilege : userRole.application().getPrivileges(applicationName)) {
                final Set<String> resources = userRole.application().getResourcePatterns(privilege);
                if (resources.isEmpty()) {
                    logger.trace("No resources defined in application privilege {}", privilege);
                } else {
                    application.add(RoleDescriptor.ApplicationResourcePrivileges.builder()
                        .application(applicationName)
                        .privileges(privilege.name())
                        .resources(resources)
                        .build());
                }
            }
        }

        final Privilege runAsPrivilege = userRole.runAs().getPrivilege();
        final Set<String> runAs;
        if (Operations.isEmpty(runAsPrivilege.getAutomaton())) {
            runAs = Collections.emptySet();
        } else {
            runAs = runAsPrivilege.name();
        }

        return new GetUserPrivilegesResponse(cluster, conditionalCluster, indices, application, runAs);
    }

}
