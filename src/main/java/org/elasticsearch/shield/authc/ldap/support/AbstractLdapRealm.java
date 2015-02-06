/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authc.support.UsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;

import java.util.List;
import java.util.Set;

/**
 * Supporting class for JNDI-based Realms
 */
public abstract class AbstractLdapRealm extends CachingUsernamePasswordRealm {

    protected final SessionFactory sessionFactory;
    protected final GroupToRoleMapper roleMapper;

    protected AbstractLdapRealm(String type, RealmConfig config,
                                SessionFactory sessionFactory, GroupToRoleMapper roleMapper) {
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
        try (LdapSession session = sessionFactory.open(token.principal(), token.credentials())) {
            List<String> groupDNs = session.groups();
            Set<String> roles = roleMapper.mapRoles(groupDNs);
            return new User.Simple(token.principal(), roles.toArray(new String[roles.size()]));
        } catch (Throwable e) {
            if (logger.isDebugEnabled()) {
                logger.debug("authentication failed for user [{}]", e, token.principal());
            }
            return null;
        }
    }

    class Listener implements RefreshListener {
        @Override
        public void onRefresh() {
            expireAll();
        }
    }

    public static abstract class Factory<R extends AbstractLdapRealm> extends UsernamePasswordRealm.Factory<R> {

        public Factory(String type, RestController restController) {
            super(type, false, restController);
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
