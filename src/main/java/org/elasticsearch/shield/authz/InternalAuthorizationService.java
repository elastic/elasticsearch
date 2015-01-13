/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.base.Predicates;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authz.indicesresolver.DefaultIndicesResolver;
import org.elasticsearch.shield.authz.indicesresolver.IndicesResolver;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.transport.TransportRequest;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public class InternalAuthorizationService extends AbstractComponent implements AuthorizationService {

    private final Provider<ClusterService> clusterServiceProvider;
    private final RolesStore rolesStore;
    private final AuditTrail auditTrail;
    private final IndicesResolver[] indicesResolvers;

    @Inject
    //TODO replace Provider<ClusterService> with ClusterService once 1.4.1 is out, see https://github.com/elasticsearch/elasticsearch/pull/8415
    public InternalAuthorizationService(Settings settings, RolesStore rolesStore, Provider<ClusterService> clusterServiceProvider, AuditTrail auditTrail) {
        super(settings);
        this.rolesStore = rolesStore;
        this.clusterServiceProvider = clusterServiceProvider;
        this.auditTrail = auditTrail;
        this.indicesResolvers = new IndicesResolver[] {
                new DefaultIndicesResolver(this)
        };
    }

    @Override
    public ImmutableList<String> authorizedIndicesAndAliases(User user, String action) {
        String[] roles = user.roles();
        if (roles.length == 0) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Predicate<String>> predicates = ImmutableList.builder();
        for (String role : roles) {
            Permission.Global.Role global = rolesStore.role(role);
            if (global != null) {
                predicates.add(global.indices().allowedIndicesMatcher(action));
            }
        }

        ImmutableList.Builder<String> indicesAndAliases = ImmutableList.builder();
        Predicate<String> predicate = Predicates.or(predicates.build());
        MetaData metaData = clusterServiceProvider.get().state().metaData();
        for (String index : metaData.concreteAllIndices()) {
            if (predicate.apply(index)) {
                indicesAndAliases.add(index);
            }
        }
        for (Iterator<String> iter = metaData.getAliases().keysIt(); iter.hasNext(); ) {
            String alias = iter.next();
            if (predicate.apply(alias)) {
                indicesAndAliases.add(alias);
            }
        }
        return indicesAndAliases.build();
    }

    @Override
    public void authorize(User user, String action, TransportRequest request) throws AuthorizationException {

        // first we need to check if the user is the system. If it is, we'll just authorize the system access
        if (user.isSystem()) {
            if (SystemRole.INSTANCE.check(action)) {
                grant(user, action, request);
                return;
            }
            throw denial(user, action, request);
        }

        Permission.Global permission = permission(user);

        // permission can be null as it might be that the user's role
        // is unknown
        if (permission == null || permission.isEmpty()) {
            throw denial(user, action, request);
        }

        // first, we'll check if the action is a cluster action. If it is, we'll only check it
        // agaist the cluster permissions
        if (Privilege.Cluster.ACTION_MATCHER.apply(action)) {
            Permission.Cluster cluster = permission.cluster();
            if (cluster != null && cluster.check(action)) {
                grant(user, action, request);
                return;
            }
            throw denial(user, action, request);
        }

        // ok... this is not a cluster action, let's verify it's an indices action
        if (!Privilege.Index.ACTION_MATCHER.apply(action)) {
            throw denial(user, action, request);
        }

        Set<String> indexNames = resolveIndices(user, action, request);
        if (indexNames == null) {
            // the only time this will be null, is for those requests that are
            // categorized as indices request but they're actully not (for example, scroll)
            // in these cases, we only grant/deny based on the action name (performed above)
            grant(user, action, request);
            return;
        }

        Permission.Indices indices = permission.indices();
        if (indices == null || indices.isEmpty()) {
            throw denial(user, action, request);
        }

        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group
        for (String index : indexNames) {
            boolean granted = false;
            for (Permission.Indices.Group group : indices) {
                if (group.check(action, index)) {
                    granted = true;
                    break;
                }
            }
            if (!granted) {
                throw denial(user, action, request);
            }
        }

        grant(user, action, request);
    }

    private Permission.Global permission(User user) {
        String[] roleNames = user.roles();
        if (roleNames.length == 0) {
            return Permission.Global.NONE;
        }

        if (roleNames.length == 1) {
            Permission.Global.Role role = rolesStore.role(roleNames[0]);
            return role == null ? Permission.Global.NONE : role;
        }

        // we'll take all the roles and combine their associated permissions

        Permission.Global.Compound.Builder roles = Permission.Global.Compound.builder();
        for (String roleName : roleNames) {
            Permission.Global role = rolesStore.role(roleName);
            if (role != null) {
                roles.add(role);
            }
        }
        return roles.build();
    }

    private AuthorizationException denial(User user, String action, TransportRequest request) {
        auditTrail.accessDenied(user, action, request);
        return new AuthorizationException("Action [" + action + "] is unauthorized for user [" + user.principal() + "]");
    }

    private void grant(User user, String action, TransportRequest request) {
        auditTrail.accessGranted(user, action, request);
    }

    private Set<String> resolveIndices(User user, String action, TransportRequest request) {
        MetaData metaData = clusterServiceProvider.get().state().metaData();

        // some APIs are indices requests that are not actually associated with indices. For example,
        // search scroll request, is categorized under the indices context, but doesn't hold indices names
        // (in this case, the security check on the indices was done on the search request that initialized
        // the scroll... and we rely on the signed scroll id to provide security over this request).

        // so we only check indices if indeed the request is an actual IndicesRequest, if it's not, we only
        // perform the check on the action name.
        Set<String> indices = null;

        if (request instanceof IndicesRequest || request instanceof CompositeIndicesRequest) {
            indices = Collections.emptySet();
            for (IndicesResolver resolver : indicesResolvers) {
                if (resolver.requestType().isInstance(request)) {
                    indices = resolver.resolve(user, action, request, metaData);
                    break;
                }
            }
        }
        return indices;
    }
}
