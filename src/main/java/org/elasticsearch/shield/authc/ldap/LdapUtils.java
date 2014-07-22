/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.ElasticsearchException;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

public class LdapUtils {

    private LdapUtils() {
    }

    public static LdapName ldapName(String dn) {
        try {
            return new LdapName(dn);
        } catch (InvalidNameException e) {
            throw new ElasticsearchException("Invalid group DN is set [" + dn + "]", e);
        }
    }
}
