/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPInterface;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Represents a LDAP connection with an authenticated/bound user that needs closing.
 */
public class LdapSession implements Releasable {

    protected final Logger logger;
    protected final RealmConfig realm;
    protected final LDAPInterface connection;
    protected final String userDn;
    protected final GroupsResolver groupsResolver;
    private LdapMetadataResolver metadataResolver;
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
    public LdapSession(
        Logger logger,
        RealmConfig realm,
        LDAPInterface connection,
        String userDn,
        GroupsResolver groupsResolver,
        LdapMetadataResolver metadataResolver,
        TimeValue timeout,
        Collection<Attribute> attributes
    ) {
        this.logger = logger;
        this.realm = realm;
        this.connection = connection;
        this.userDn = userDn;
        this.groupsResolver = groupsResolver;
        this.metadataResolver = metadataResolver;
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
        if (connection instanceof LDAPConnection) {
            ((LDAPConnection) connection).close();
        }
    }

    /**
     * @return the fully distinguished name of the user bound to this connection
     */
    public String userDn() {
        return userDn;
    }

    /**
     * @return the realm for which this session was created
     */
    public RealmConfig realm() {
        return realm;
    }

    /**
     * @return the connection to the LDAP/AD server of this session
     */
    public LDAPInterface getConnection() {
        return connection;
    }

    /**
     * Asynchronously retrieves a list of group distinguished names
     */
    public void groups(ActionListener<List<String>> listener) {
        groupsResolver.resolve(connection, userDn, timeout, logger, attributes, listener);
    }

    public void metadata(ActionListener<Map<String, Object>> listener) {
        metadataResolver.resolve(connection, userDn, timeout, logger, attributes, listener);
    }

    public void resolve(ActionListener<LdapUserData> listener) {
        logger.debug("Resolving LDAP groups + meta-data for user [{}]", userDn);
        groups(ActionListener.wrap(groups -> {
            logger.debug("Resolved {} LDAP groups [{}] for user [{}]", groups.size(), groups, userDn);
            metadata(ActionListener.wrap(meta -> {
                logger.debug("Resolved {} meta-data fields [{}] for user [{}]", meta.size(), meta, userDn);
                listener.onResponse(new LdapUserData(groups, meta));
            }, listener::onFailure));
        }, listener::onFailure));
    }

    public static class LdapUserData {
        public final List<String> groups;
        public final Map<String, Object> metadata;

        public LdapUserData(List<String> groups, Map<String, Object> metadata) {
            this.groups = groups;
            this.metadata = metadata;
        }
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
        void resolve(
            LDAPInterface ldapConnection,
            String userDn,
            TimeValue timeout,
            Logger logger,
            Collection<Attribute> attributes,
            ActionListener<List<String>> listener
        );

        /**
         * Returns the attributes that this resolvers uses. If no attributes are required, return {@code null}.
         */
        String[] attributes();
    }
}
