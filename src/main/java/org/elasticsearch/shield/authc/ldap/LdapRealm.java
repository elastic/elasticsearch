/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapRealm;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends AbstractLdapRealm {

    public static final String TYPE = "ldap";

    @Inject
    public LdapRealm(Settings settings,
                     GenericLdapConnectionFactory ldap,
                     LdapGroupToRoleMapper roleMapper,
                     RestController restController) {
        super(settings, ldap, roleMapper, restController);
    }

    @Override
    public String type() {
        return TYPE;
    }
}
