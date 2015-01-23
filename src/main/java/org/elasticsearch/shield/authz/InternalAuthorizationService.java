/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestHelper;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.base.Predicates;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authz.indicesresolver.DefaultIndicesResolver;
import org.elasticsearch.shield.authz.indicesresolver.IndicesResolver;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.transport.TransportRequest;

import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public class InternalAuthorizationService extends AbstractComponent implements AuthorizationService {

    private final ClusterService clusterService;
    private final RolesStore rolesStore;
    private final AuditTrail auditTrail;
    private final IndicesResolver[] indicesResolvers;

    @Inject
    public InternalAuthorizationService(Settings settings, RolesStore rolesStore, ClusterService clusterService, AuditTrail auditTrail) {
        super(settings);
        this.rolesStore = rolesStore;
        this.clusterService = clusterService;
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
        MetaData metaData = clusterService.state().metaData();
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
        // against the cluster permissions
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

        // some APIs are indices requests that are not actually associated with indices. For example,
        // search scroll request, is categorized under the indices context, but doesn't hold indices names
        // (in this case, the security check on the indices was done on the search request that initialized
        // the scroll... and we rely on the signed scroll id to provide security over this request).
        // so we only check indices if indeed the request is an actual IndicesRequest, if it's not,
        // we just grant it if it's a scroll, deny otherwise
        if (!(request instanceof IndicesRequest) && !(request instanceof CompositeIndicesRequest)) {
            if (isScrollRelatedAction(action)) {
                //note that clear scroll shard level actions can originate from a clear scroll all, which doesn't require any
                //indices permission as it's categorized under cluster. This is why the scroll check is performed
                //even before checking if the user has any indices permission.
                grant(user, action, request);
                return;
            }
            assert false : "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
            throw denial(user, action, request);
        }

        if (permission.indices() == null || permission.indices().isEmpty()) {
            throw denial(user, action, request);
        }

        Set<String> indexNames = resolveIndices(user, action, request);
        assert !indexNames.isEmpty() : "every indices request needs to have its indices set thus the resolved indices must not be empty";

        if (!authorizeIndices(action, indexNames, permission.indices())) {
            throw denial(user, action, request);
        }

        //if we are creating an index we need to authorize potential aliases created at the same time
        if (Privilege.Index.CREATE_INDEX.predicate().apply(action)) {
            assert request instanceof CreateIndexRequest;
            Set<Alias> aliases = CreateIndexRequestHelper.aliases((CreateIndexRequest) request);
            if (!aliases.isEmpty()) {
                Set<String> aliasesAndIndices = Sets.newHashSet(indexNames);
                for (Alias alias : aliases) {
                    aliasesAndIndices.add(alias.name());
                }
                if (!authorizeIndices("indices:admin/aliases", aliasesAndIndices, permission.indices())) {
                    throw denial(user, "indices:admin/aliases", request);
                }
            }
        }

        grant(user, action, request);
    }

    private boolean authorizeIndices(String action, Set<String> requestIndices, Permission.Indices permission) {
        // now... every index that is associated with the request, must be granted
        // by at least one indices permission group
        for (String index : requestIndices) {
            boolean granted = false;
            for (Permission.Indices.Group group : permission) {
                if (group.check(action, index)) {
                    granted = true;
                    break;
                }
            }
            if (!granted) {
                return false;
            }
        }
        return true;
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

    private Set<String> resolveIndices(User user, String action, TransportRequest request) {
        MetaData metaData = clusterService.state().metaData();
        for (IndicesResolver resolver : indicesResolvers) {
            if (resolver.requestType().isInstance(request)) {
                return resolver.resolve(user, action, request, metaData);
            }
        }
        assert false : "we should be able to resolve indices for any known request that requires indices privileges";
        throw denial(user, action, request);
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME) ||
                action.equals(SearchServiceTransportAction.SCAN_SCROLL_ACTION_NAME) ||
                action.equals(SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME) ||
                action.equals(SearchServiceTransportAction.QUERY_FETCH_SCROLL_ACTION_NAME) ||
                action.equals(SearchServiceTransportAction.QUERY_SCROLL_ACTION_NAME) ||
                action.equals(SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME) ||
                action.equals(ClearScrollAction.NAME) ||
                action.equals(SearchServiceTransportAction.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
    }

    private AuthorizationException denial(User user, String action, TransportRequest request) {
        auditTrail.accessDenied(user, action, request);
        return new AuthorizationException("action [" + action + "] is unauthorized for user [" + user.principal() + "]");
    }

    private void grant(User user, String action, TransportRequest request) {
        auditTrail.accessGranted(user, action, request);
    }

}
