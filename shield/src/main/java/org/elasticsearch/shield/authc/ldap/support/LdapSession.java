/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPInterface;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;

import java.io.Closeable;
import java.util.List;

/**
 * Represents a LDAP connection with an authenticated/bound user that needs closing.
 */
public class LdapSession implements Closeable {

    protected final ESLogger logger;
    protected final LDAPInterface ldap;
    protected final String userDn;
    protected final GroupsResolver groupsResolver;
    protected final TimeValue timeout;

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     *
     * This constructor accepts a logger with which the connection can log. Since this connection
     * can be instantiated very frequently, it's best to have the logger for this connection created
     * outside of and be reused across all connections. We can't keep a static logger in this class
     * since we want the logger to be contextual (i.e. aware of the settings and its environment).
     */
    public LdapSession(ESLogger logger, LDAPInterface connection, String userDn, GroupsResolver groupsResolver, TimeValue timeout) {
        this.logger = logger;
        this.ldap = connection;
        this.userDn = userDn;
        this.groupsResolver = groupsResolver;
        this.timeout = timeout;
    }

    /**
     * LDAP connections should be closed to clean up resources.
     */
    @Override
    public void close() {
        // Only if it is an LDAPConnection do we need to close it
        if (ldap instanceof LDAPConnection) {
            ((LDAPConnection) ldap).close();
        }
    }

    /**
     * @return the fully distinguished name of the user bound to this connection
     */
    public String userDn() {
        return userDn;
    }

    /**
     * @return List of fully distinguished group names
     */
    public List<String> groups() {
        return groupsResolver.resolve(ldap, userDn, timeout, logger);
    }

    public static interface GroupsResolver {

        List<String> resolve(LDAPInterface ldapConnection, String userDn, TimeValue timeout, ESLogger logger);

    }
}
