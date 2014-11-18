/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapRealm;

/**
 *
 */
public class ActiveDirectoryRealm extends AbstractLdapRealm {

    public static final String type = "active_directory";

    @Inject
    public ActiveDirectoryRealm(Settings settings,
                                ActiveDirectoryConnectionFactory connectionFactory,
                                ActiveDirectoryGroupToRoleMapper roleMapper,
                                RestController restController) {
        super(settings, connectionFactory, roleMapper, restController);
    }

    @Override
    public String type() {
        return type;
    }
}
