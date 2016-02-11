/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.shield.action.realm.ClearRealmCacheAction;
import org.elasticsearch.shield.action.role.ClearRolesCacheAction;
import org.elasticsearch.shield.authz.permission.Role;
import org.elasticsearch.shield.authz.privilege.ClusterPrivilege;
import org.elasticsearch.shield.authz.privilege.IndexPrivilege;
import org.elasticsearch.shield.authz.privilege.Privilege;

/**
 * XPack internal user that manages xpack. Has all cluster/indices permissions for watcher,
 * shield and monitoring to operate.
 */
public class XPackUser extends User {

    public static final String NAME = "__es_internal_user";

    public static final Role ROLE = Role.builder("__es_internal_role")
            .cluster(ClusterPrivilege.get(new Privilege.Name(
                    ClearRealmCacheAction.NAME + "*",       // shield
                    ClearRolesCacheAction.NAME + "*",       // shield
                    PutIndexTemplateAction.NAME,            // shield, marvel, watcher
                    GetIndexTemplatesAction.NAME + "*",     // marvel
                    ClusterPrivilege.MONITOR.name().toString())))  // marvel


            // for now, the watches will be executed under the watcher user, meaning, all actions
            // taken as part of the execution will be executed on behalf of this user. this includes
            // the index action, search input and search transform. For this reason the watcher user
            // requires full access to all indices in the cluster.
            //
            // at later phases we'll want to execute the watch on behalf of the user who registers
            // it. this will require some work to attach/persist that user to/with the watch.
            .add(IndexPrivilege.ALL, "*")


//            these will be the index permissions required by shield (will uncomment once we optimize watcher permissions)

//            .add(IndexPrivilege.ALL, ShieldTemplateService.SHIELD_ADMIN_INDEX_NAME)
//            .add(IndexPrivilege.ALL, IndexAuditTrail.INDEX_NAME_PREFIX + "*")



//            these will be the index permissions required by monitoring (will uncomment once we optimize monitoring permissions)

//            // we need all monitoring access
//            .add(IndexPrivilege.MONITOR, "*")
//            // and full access to .monitoring-es-* and .monitoring-es-data indices
//            .add(IndexPrivilege.ALL, MarvelSettings.MONITORING_INDICES_PREFIX + "*")

            .build();

    public static final XPackUser INSTANCE = new XPackUser();

    XPackUser() {
        super(NAME, ROLE.name());
    }

    @Override
    public boolean equals(Object o) {
        return INSTANCE == o;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    public static boolean is(User user) {
        return INSTANCE.equals(user);
    }
}
