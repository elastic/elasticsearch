/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.shield.authc.ldap.LdapException;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

public final class LdapUtils {

    private LdapUtils() {
    }

    public static LdapName ldapName(String dn) {
        try {
            return new LdapName(dn);
        } catch (InvalidNameException e) {
            throw new LdapException("invalid group DN [" + dn + "]", e);
        }
    }
}
