/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.shield.authc.active_directory.ActiveDirectoryRealm;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;

import java.util.ArrayList;
import java.util.List;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms {

    private static final ESLogger logger = ESLoggerFactory.getLogger(Realms.class.getName());

    private final Realm[] realms;

    @Inject
    public Realms(@Nullable ESUsersRealm esusers, @Nullable LdapRealm ldap, @Nullable ActiveDirectoryRealm activeDirectory) {

        List<Realm> realms = new ArrayList<>();
        if (esusers != null) {
            logger.info("Realm [" + esusers.type() + "] is used");
            realms.add(esusers);
        }
        if (ldap != null) {
            logger.info("Realm [" + ldap.type() + "] is used");
            realms.add(ldap);
        }
        if (activeDirectory != null) {
            logger.info("Realm [" + activeDirectory.type() + "] is used");
            realms.add(activeDirectory);
        }
        this.realms = realms.toArray(new Realm[realms.size()]);
    }

    Realm[] realms() {
        return realms;
    }
}
