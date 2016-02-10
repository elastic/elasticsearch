/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authc.support.UsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;

import java.util.List;
import java.util.Set;

/**
 * Supporting class for LDAP realms
 */
public abstract class AbstractLdapRealm extends CachingUsernamePasswordRealm {

    protected final SessionFactory sessionFactory;
    protected final DnRoleMapper roleMapper;

    protected AbstractLdapRealm(String type, RealmConfig config,
                                SessionFactory sessionFactory, DnRoleMapper roleMapper) {
        super(type, config);
        this.sessionFactory = sessionFactory;
        this.roleMapper = roleMapper;
        roleMapper.addListener(new Listener());
    }

    /**
     * Given a username and password, open to ldap, retrieve groups, map to roles and build the user.
     *
     * @return User with elasticsearch roles
     */
    @Override
    protected User doAuthenticate(UsernamePasswordToken token) {
        try (LdapSession session = sessionFactory.session(token.principal(), token.credentials())) {
            return createUser(token.principal(), session);
        } catch (Exception e) {
            logException("authentication", e, token.principal());
            return null;
        }
    }

    @Override
    public User doLookupUser(String username) {
        if (sessionFactory.supportsUnauthenticatedSession()) {
            try (LdapSession session = sessionFactory.unauthenticatedSession(username)) {
                return createUser(username, session);
            } catch (Exception e) {
                logException("lookup", e, username);
            }
        }
        return null;
    }

    @Override
    public boolean userLookupSupported() {
        return sessionFactory.supportsUnauthenticatedSession();
    }

    private void logException(String action, Exception e, String principal) {
        if (logger.isDebugEnabled()) {
            logger.debug("{} failed for user [{}]", e, action, principal);
        } else {
            String causeMessage = (e.getCause() == null) ? null : e.getCause().getMessage();
            if (causeMessage == null) {
                logger.warn("{} failed for user [{}]: {}", action, principal, e.getMessage());
            } else {
                logger.warn("{} failed for user [{}]: {}\ncause: {}: {}", action, principal, e.getMessage(),
                        e.getCause().getClass().getName(), causeMessage);
            }
        }
    }

    private User createUser(String principal, LdapSession session) {
        List<String> groupDNs = session.groups();
        Set<String> roles = roleMapper.resolveRoles(session.userDn(), groupDNs);
        return new User(principal, roles.toArray(new String[roles.size()]));
    }

    class Listener implements RefreshListener {
        @Override
        public void onRefresh() {
            expireAll();
        }
    }

    public static abstract class Factory<R extends AbstractLdapRealm> extends UsernamePasswordRealm.Factory<R> {

        public Factory(String type, RestController restController) {
            super(type, restController, false);
        }

        /**
         * LDAP realms require minimum settings (e.g. URL), therefore they'll never create a default.
         *
         * @return {@code null} always
         */
        @Override
        public final R createDefault(String name) {
            return null;
        }
    }
}
