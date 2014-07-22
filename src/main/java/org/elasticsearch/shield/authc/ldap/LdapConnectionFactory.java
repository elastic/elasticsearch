/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

/**
 * This factory holds settings needed for authenticating to LDAP and creating LdapConnections.
 * Each created LdapConnection needs to be closed or else connections will pill up consuming resources.
 *
 * A standard looking usage pattern could look like this:
 <pre>
    try (LdapConnection session = ldapFac.bindXXX(...);
        ...do stuff with the session
    }
 </pre>
 */
public interface LdapConnectionFactory {

    public static final String URLS_SETTING = "urls"; //comma separated

    /**
     * Password authenticated bind
     * @param user name of the user to authenticate the connection with.
     */
    public LdapConnection bind(String user, char[] password) ;

}
