/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.authc.system.SystemRealm;

import java.util.ArrayList;
import java.util.List;

/**
 * Serves as a realms registry (also responsible for ordering the realms appropriately)
 */
public class Realms {

    private final Realm[] realms;

    @Inject
    public Realms(SystemRealm system, @Nullable ESUsersRealm esusers, @Nullable LdapRealm ldap) {
        List<Realm> realms = new ArrayList<>();
        realms.add(system);
        if (esusers != null) {
            realms.add(esusers);
        }
        if (ldap != null) {
            realms.add(ldap);
        }
        this.realms = realms.toArray(new Realm[realms.size()]);
    }

    Realm[] realms() {
        return realms;
    }

}
