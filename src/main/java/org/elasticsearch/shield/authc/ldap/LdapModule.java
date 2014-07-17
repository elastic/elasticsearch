/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public class LdapModule extends AbstractModule {

    public static boolean enabled(Settings settings) {
        Settings ldapSettings = settings.getComponentSettings(LdapModule.class);
        return ldapSettings != null;
    }

    @Override
    protected void configure() {

    }
}
