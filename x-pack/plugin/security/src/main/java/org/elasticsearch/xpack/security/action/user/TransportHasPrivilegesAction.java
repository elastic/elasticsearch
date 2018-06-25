/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Transport action that tests whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class TransportHasPrivilegesAction extends HandledTransportAction<HasPrivilegesRequest, HasPrivilegesResponse> {

    private final ThreadPool threadPool;
    private final AuthorizationService authorizationService;

    @Inject
    public TransportHasPrivilegesAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                        ActionFilters actionFilters, AuthorizationService authorizationService) {
        super(settings, HasPrivilegesAction.NAME, transportService, actionFilters, HasPrivilegesRequest::new);
        this.threadPool = threadPool;
        this.authorizationService = authorizationService;
    }

    @Override
    protected void doExecute(Task task, HasPrivilegesRequest request, ActionListener<HasPrivilegesResponse> listener) {
        final String username = request.username();

        final User user = Authentication.getAuthentication(threadPool.getThreadContext()).getUser();
        if (user.principal().equals(username) == false) {
            listener.onFailure(new IllegalArgumentException("users may only check the privileges of their own account"));
            return;
        }

        authorizationService.roles(user, ActionListener.wrap(
                role -> checkPrivileges(request, role, listener),
                listener::onFailure));
    }

    private void checkPrivileges(HasPrivilegesRequest request, Role userRole,
                                 ActionListener<HasPrivilegesResponse> listener) {
        logger.debug(() -> new ParameterizedMessage("Check whether role [{}] has privileges cluster=[{}] index=[{}]",
                Strings.arrayToCommaDelimitedString(userRole.names()), Strings.arrayToCommaDelimitedString(request.clusterPrivileges()),
                Strings.arrayToCommaDelimitedString(request.indexPrivileges())));

        Map<String, Boolean> cluster = new HashMap<>();
        for (String checkAction : request.clusterPrivileges()) {
            final ClusterPrivilege checkPrivilege = ClusterPrivilege.get(Collections.singleton(checkAction));
            final ClusterPrivilege rolePrivilege = userRole.cluster().privilege();
            cluster.put(checkAction, testPrivilege(checkPrivilege, rolePrivilege.getAutomaton()));
        }
        boolean allMatch = cluster.values().stream().allMatch(Boolean::booleanValue);

        final Map<IndicesPermission.Group, Automaton> predicateCache = new HashMap<>();

        final Map<String, HasPrivilegesResponse.IndexPrivileges> indices = new LinkedHashMap<>();
        for (RoleDescriptor.IndicesPrivileges check : request.indexPrivileges()) {
            for (String index : check.getIndices()) {
                final Map<String, Boolean> privileges = new HashMap<>();
                final HasPrivilegesResponse.IndexPrivileges existing = indices.get(index);
                if (existing != null) {
                    privileges.putAll(existing.getPrivileges());
                }
                for (String privilege : check.getPrivileges()) {
                    if (testIndexMatch(index, privilege, userRole, predicateCache)) {
                        logger.debug(() -> new ParameterizedMessage("Role [{}] has [{}] on [{}]",
                                Strings.arrayToCommaDelimitedString(userRole.names()), privilege, index));
                        privileges.put(privilege, true);
                    } else {
                        logger.debug(() -> new ParameterizedMessage("Role [{}] does not have [{}] on [{}]",
                                Strings.arrayToCommaDelimitedString(userRole.names()), privilege, index));
                        privileges.put(privilege, false);
                        allMatch = false;
                    }
                }
                indices.put(index, new HasPrivilegesResponse.IndexPrivileges(index, privileges));
            }
        }
        listener.onResponse(new HasPrivilegesResponse(allMatch, cluster, indices.values()));
    }

    private boolean testIndexMatch(String checkIndex, String checkPrivilegeName, Role userRole,
                                   Map<IndicesPermission.Group, Automaton> predicateCache) {
        final IndexPrivilege checkPrivilege = IndexPrivilege.get(Collections.singleton(checkPrivilegeName));

        final Automaton checkIndexAutomaton = Automatons.patterns(checkIndex);

        List<Automaton> privilegeAutomatons = new ArrayList<>();
        for (IndicesPermission.Group group : userRole.indices().groups()) {
            final Automaton groupIndexAutomaton = predicateCache.computeIfAbsent(group, g -> Automatons.patterns(g.indices()));
            if (testIndex(checkIndexAutomaton, groupIndexAutomaton)) {
                final IndexPrivilege rolePrivilege = group.privilege();
                if (rolePrivilege.name().contains(checkPrivilegeName)) {
                    return true;
                }
                privilegeAutomatons.add(rolePrivilege.getAutomaton());
            }
        }
        return testPrivilege(checkPrivilege, Automatons.unionAndMinimize(privilegeAutomatons));
    }

    private static boolean testIndex(Automaton checkIndex, Automaton roleIndex) {
        return Operations.subsetOf(checkIndex, roleIndex);
    }

    private static boolean testPrivilege(Privilege checkPrivilege, Automaton roleAutomaton) {
        return Operations.subsetOf(checkPrivilege.getAutomaton(), roleAutomaton);
    }
}
