/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPInterface;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Collection;
import java.util.List;

/**
 * Represents a LDAP connection with an authenticated/bound user that needs closing.
 */
public class LdapSession implements Releasable {

    protected final Logger logger;
    protected final LDAPInterface ldap;
    protected final String userDn;
    protected final GroupsResolver groupsResolver;
    protected final TimeValue timeout;
    protected final Collection<Attribute> attributes;

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     *
     * This constructor accepts a logger with which the connection can log. Since this connection
     * can be instantiated very frequently, it's best to have the logger for this connection created
     * outside of and be reused across all connections. We can't keep a static logger in this class
     * since we want the logger to be contextual (i.e. aware of the settings and its environment).
     */
    public LdapSession(Logger logger, LDAPInterface connection, String userDn, GroupsResolver groupsResolver, TimeValue timeout,
                       Collection<Attribute> attributes) {
        this.logger = logger;
        this.ldap = connection;
        this.userDn = userDn;
        this.groupsResolver = groupsResolver;
        this.timeout = timeout;
        this.attributes = attributes;
    }

    /**
     * LDAP connections should be closed to clean up resources.
     */
    @Override
    public void close() {
        // Only if it is an LDAPConnection do we need to close it, otherwise it is a connection pool and we will close all of the
        // connections in the pool
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
     * Asynchronously retrieves a list of group distinguished names
     */
    public void groups(ActionListener<List<String>> listener) {
        groupsResolver.resolve(ldap, userDn, timeout, logger, attributes, listener);
    }

    /**
     * A GroupsResolver is used to resolve the group names of a given LDAP user
     */
    public interface GroupsResolver {

        /**
         * Asynchronously resolve the group name for the given ldap user
         * @param ldapConnection an authenticated {@link LDAPConnection} to be used for LDAP queries
         * @param userDn the distinguished name of the ldap user
         * @param timeout the timeout for any ldap operation
         * @param logger the logger to use if necessary
         * @param attributes a collection of attributes that were previously retrieved for the user such as during a user search.
         *          {@code null} indicates that the attributes have not been attempted to be retrieved
         * @param listener the listener to call on a result or on failure
         */
        void resolve(LDAPInterface ldapConnection, String userDn, TimeValue timeout, Logger logger, Collection<Attribute> attributes,
                     ActionListener<List<String>> listener);

        /**
         * Returns the attributes that this resolvers uses. If no attributes are required, return {@code null}.
         */
        String[] attributes();
    }
}
