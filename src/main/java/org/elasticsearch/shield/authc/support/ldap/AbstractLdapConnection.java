/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.logging.ESLogger;

import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import java.io.Closeable;
import java.util.List;

/**
 * Represents a LDAP connection with an authenticated/bound user that needs closing.
 */
public abstract class AbstractLdapConnection implements Closeable {

    protected final ESLogger logger;
    protected final DirContext jndiContext;
    protected final String bindDn;

    /**
     * This object is intended to be constructed by the LdapConnectionFactory
     *
     * This constructor accepts a logger with wich the connection can log. Since this connection
     * can be instantiated very frequently, it's best to have the logger for this connection created
     * outside of and be reused across all connections. We can't keep a static logger in this class
     * since we want the logger to be contextual (i.e. aware of the settings and its enviorment).
     */
    public AbstractLdapConnection(ESLogger logger, DirContext ctx, String boundName) {
        this.logger = logger;
        this.jndiContext = ctx;
        this.bindDn = boundName;
    }

    /**
     * LDAP connections should be closed to clean up resources.  However, the jndi contexts have the finalize
     * implemented properly so that it will clean up on garbage collection.
     */
    @Override
    public void close(){
        try {
            jndiContext.close();
        } catch (NamingException e) {
            throw new SecurityException("could not close the LDAP connection", e);
        }
    }

    /**
     * @return the fully distinguished name of the user bound to this connection
     */
    public String authenticatedUserDn() {
        return bindDn;
    }

    /**
     * @return List of fully distinguished group names
     */
    public abstract List<String> groups();
}
