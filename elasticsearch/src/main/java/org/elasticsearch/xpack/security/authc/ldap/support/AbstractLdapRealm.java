/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPException;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.authc.support.RefreshListener;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;

import java.util.List;
import java.util.Map;
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
     * Given a username and password, open a connection to ldap, bind to authenticate, retrieve groups, map to roles and build the user.
     * This user will then be passed to the listener
     */
    @Override
    protected final void doAuthenticate(UsernamePasswordToken token, ActionListener<User> listener) {
        // we use a runnable so that we call the listener outside of the try catch block. If we call within the try catch block and the
        // listener throws an exception then we mistakenly could continue realm authentication when we already authenticated the user and
        // there was some other issue
        Runnable action;
        try (LdapSession session = sessionFactory.session(token.principal(), token.credentials())) {
            final User user = createUser(token.principal(), session);
            action = () -> listener.onResponse(user);
        } catch (Exception e) {
            logException("authentication", e, token.principal());
            action = () -> listener.onResponse(null);
        }
        action.run();
    }

    @Override
    protected final void doLookupUser(String username, ActionListener<User> listener) {
        // we use a runnable so that we call the listener outside of the try catch block. If we call within the try catch block and the
        // listener throws an exception then we mistakenly could continue realm lookup when we already found a matching user and
        // there was some other issue
        Runnable action;
        if (sessionFactory.supportsUnauthenticatedSession()) {
            try (LdapSession session = sessionFactory.unauthenticatedSession(username)) {
                final User user = createUser(username, session);
                action = () -> listener.onResponse(user);
            } catch (Exception e) {
                logException("lookup", e, username);
                action = () -> listener.onResponse(null);
            }
        } else {
            action = () -> listener.onResponse(null);
        }
        action.run();
    }

    @Override
    public boolean userLookupSupported() {
        return sessionFactory.supportsUnauthenticatedSession();
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> usage = super.usageStats();
        usage.put("load_balance_type", LdapLoadBalancing.resolve(config.settings()).toString());
        usage.put("ssl", sessionFactory.sslUsed);
        return usage;
    }

    private void logException(String action, Exception e, String principal) {
        if (logger.isDebugEnabled()) {
            logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed for user [{}]", action, principal), e);
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

    private User createUser(String principal, LdapSession session) throws LDAPException {
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
}
