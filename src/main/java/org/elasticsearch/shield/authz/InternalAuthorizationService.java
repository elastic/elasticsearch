/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.transport.TransportRequest;

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
    public void authorize(User user, String action, TransportRequest request) throws AuthorizationException {
        Permission permission = rolesStore.permission(user.roles());
        MetaData metaData = clusterService.state().metaData();
        if (permission.check(action, request, metaData)) {
            if (auditTrail != null) {
                auditTrail.accessGranted(user, action, request);
            }
            return;
        }
        auditTrail.accessDenied(user, action, request);
        throw new AuthorizationException("Action [" + action + "] is unauthorized");
    }
}
