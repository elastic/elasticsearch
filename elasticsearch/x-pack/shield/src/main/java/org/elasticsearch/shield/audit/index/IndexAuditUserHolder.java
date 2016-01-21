/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit.index;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.IndexPrivilege;

/**
 *
 */
public class IndexAuditUserHolder {

    private static final String NAME = "__indexing_audit_user";
    private static final String[] ROLE_NAMES = new String[] { "__indexing_audit_role" };
    public static final Role ROLE = Role.builder(ROLE_NAMES[0])
        .cluster(ClusterPrivilege.action(PutIndexTemplateAction.NAME))
        .add(IndexPrivilege.CREATE_INDEX, IndexAuditTrail.INDEX_NAME_PREFIX + "*")
        .add(IndexPrivilege.INDEX, IndexAuditTrail.INDEX_NAME_PREFIX + "*")
        .add(IndexPrivilege.action(IndicesExistsAction.NAME), IndexAuditTrail.INDEX_NAME_PREFIX + "*")
        .add(IndexPrivilege.action(BulkAction.NAME), IndexAuditTrail.INDEX_NAME_PREFIX + "*")
        .add(IndexPrivilege.action(PutMappingAction.NAME), IndexAuditTrail.INDEX_NAME_PREFIX + "*")
        .build();

    private final User user;

    public IndexAuditUserHolder() {
        this.user = new User(NAME, ROLE_NAMES);
    }

    public User user() {
        return user;
    }
}
