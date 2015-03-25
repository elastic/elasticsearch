/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;

/**
 *
 */
public class WatcherUserHolder {

    static final String NAME = "__watcher_user";
    static final String[] ROLE_NAMES = new String[] { "__watcher_role" };

    final Permission.Global.Role role = Permission.Global.Role.builder(ROLE_NAMES[0])
            .set(Privilege.Cluster.action("indices:admin/template/put"))

                    // for now, the watches will be executed under the watcher user, meaning, all actions
                    // taken as part of the execution will be executed on behalf of this user. this includes
                    // the index action, search input and search transform. For this reason the watcher user
                    // requires full access to all indices in the cluster.
                    //
                    // at later phases we'll want to execute the watch on behalf of the user who registers
                    // it. this will require some work to attache/persist that user to/with the watch.
            .add(Privilege.Index.ALL, "*")

            .build();

    final User user = new User.Simple(NAME, ROLE_NAMES);

}
