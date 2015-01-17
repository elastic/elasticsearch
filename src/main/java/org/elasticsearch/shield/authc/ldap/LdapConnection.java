/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapConnection;

import javax.naming.directory.DirContext;

/**
 * Encapsulates jndi/ldap functionality into one authenticated connection.  The constructor is package scoped, assuming
 * instances of this connection will be produced by the LdapConnectionFactory.open() methods.
 * <p/>
 * A standard looking usage pattern could look like this:
 * <pre>
 * try (LdapConnection session = ldapFac.bindXXX(...);
 * ...do stuff with the session
 * }
 * </pre>
 */
public class LdapConnection extends AbstractLdapConnection {

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     */
    LdapConnection(ESLogger logger, DirContext ctx, String bindDN, AbstractLdapConnection.GroupsResolver groupsResolver, TimeValue timeout) {
        super(logger, ctx, bindDN, groupsResolver, timeout);
    }
}
