/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authz.Permission;
import org.elasticsearch.shield.authz.Privilege;

/**
 *
 */
public class InternalMarvelUser extends User.Simple {

    static final String NAME = "__marvel_user";
    static final String[] ROLE_NAMES = new String[] { "__marvel_role" };

    public static final InternalMarvelUser INSTANCE = new InternalMarvelUser(NAME, ROLE_NAMES);

    public static final Permission.Global.Role ROLE = Permission.Global.Role.builder(ROLE_NAMES[0])
            .cluster(Privilege.Cluster.get(new Privilege.Name(
                    PutIndexTemplateAction.NAME + "*",
                    GetIndexTemplatesAction.NAME + "*",
                    Privilege.Cluster.MONITOR.name().toString())))

            // we need all monitoring access
            .add(Privilege.Index.MONITOR, "*")

            // and full access to .marvel-* and .marvel-data indices
            .add(Privilege.Index.ALL, MarvelSettings.MARVEL_INDICES_PREFIX + "*")

            // note, we don't need _license permission as we're taking the licenses
            // directly form the license service.

            .build();

    InternalMarvelUser(String username, String[] roles) {
        super(username, roles);
    }
}
