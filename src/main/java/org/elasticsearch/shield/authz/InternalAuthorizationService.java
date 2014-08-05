/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.base.Predicates;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authz.store.RolesStore;
import org.elasticsearch.transport.TransportRequest;

import java.util.Iterator;

/**
 *
 */
public class InternalAuthorizationService extends AbstractComponent implements AuthorizationService {

    private final ClusterService clusterService;
    private final RolesStore rolesStore;
    private final @Nullable AuditTrail auditTrail;

    @Inject
    public InternalAuthorizationService(Settings settings, RolesStore rolesStore, ClusterService clusterService, @Nullable AuditTrail auditTrail) {
        super(settings);
        this.rolesStore = rolesStore;
        this.clusterService = clusterService;
        this.auditTrail = auditTrail;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ImmutableList<String> authorizedIndicesAndAliases(User user, String action) {
        String[] roles = user.roles();
        if (roles.length == 0) {
            return ImmutableList.of();
        }
        Predicate[] predicates = new Predicate[roles.length];
        for (int i = 0; i < roles.length; i++) {
            Permission.Global global = rolesStore.permission(roles[i]);
            predicates[i] = global.indices().allowedIndicesMatcher(action);
        }
        ImmutableList.Builder<String> indicesAndAliases = ImmutableList.builder();
        Predicate<String> predicate = Predicates.or(predicates);
        MetaData metaData = clusterService.state().metaData();
        for (String index : metaData.concreteAllIndices()) {
            if (predicate.apply(index)) {
                indicesAndAliases.add(index);
            }
        }
        for (Iterator<String> iter = metaData.getAliases().keysIt(); iter.hasNext();) {
            String alias = iter.next();
            if (predicate.apply(alias)) {
                indicesAndAliases.add(alias);
            }
        }
        return indicesAndAliases.build();
    }

    @Override
    public void authorize(User user, String action, TransportRequest request) throws AuthorizationException {
        String[] roles = user.roles();
        if (roles.length == 0) {
            deny(user, action, request);
        }

        MetaData metaData = clusterService.state().metaData();
        for (String role : roles) {
            Permission permission = rolesStore.permission(role);
            if (permission.check(action, request, metaData)) {
                grant(user, action, request);
                return;
            }
        }

        deny(user, action, request);
    }

    private void deny(User user, String action, TransportRequest request) {
        if (auditTrail != null) {
            auditTrail.accessDenied(user, action, request);
        }
        throw new AuthorizationException("Action [" + action + "] is unauthorized");
    }

    private void grant(User user, String action, TransportRequest request) {
        if (auditTrail != null) {
            auditTrail.accessGranted(user, action, request);
        }
    }
}
