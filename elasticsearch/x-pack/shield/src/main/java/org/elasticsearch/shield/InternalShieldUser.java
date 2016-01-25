/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.IndexPrivilege;
import org.elasticsearch.shield.authz.privilege.Privilege;

/**
 * Shield internal user that manages the {@code .shield}
 * index. Has permission to monitor the cluster as well as all actions that deal
 * with the shield admin index.
 */
public class InternalShieldUser extends User {

    public static final String NAME = "__es_internal_user";

    public static final Role ROLE = Role.builder("__es_internal_role")
            .cluster(ClusterPrivilege.get(new Privilege.Name(PutIndexTemplateAction.NAME, "cluster:admin/shield/realm/cache/clear*", "cluster:admin/shield/roles/cache/clear*")))
            .add(IndexPrivilege.ALL, ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME)
            .add(IndexPrivilege.ALL, IndexAuditTrail.INDEX_NAME_PREFIX + "*")
            .build();

    public static final InternalShieldUser INSTANCE = new InternalShieldUser();

    InternalShieldUser() {
        super(NAME, ROLE.name());
    }

    @Override
    public boolean equals(Object o) {
        return INSTANCE == o;
    }

    public static boolean is(User user) {
        return INSTANCE.equals(user);
    }
}
