/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.RefreshListener;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.transport.TransportMessage;

import java.util.List;
import java.util.Set;

/**
 * Supporting class for JNDI-based Realms
 */
public abstract class AbstractLdapRealm extends CachingUsernamePasswordRealm implements Realm<UsernamePasswordToken> {

    protected final ConnectionFactory connectionFactory;
    protected final AbstractGroupToRoleMapper roleMapper;

    protected AbstractLdapRealm(Settings settings, ConnectionFactory ldap, AbstractGroupToRoleMapper roleMapper, RestController restController) {
        super(settings);
        this.connectionFactory = ldap;
        this.roleMapper = roleMapper;
        roleMapper.addListener(new Listener());
        restController.registerRelevantHeaders(UsernamePasswordToken.BASIC_AUTH_HEADER);
    }

    @Override
    public UsernamePasswordToken token(TransportMessage<?> message) {
        return UsernamePasswordToken.extractToken(message, null);
    }

    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    /**
     * Given a username and password, open to ldap, retrieve groups, map to roles and build the user.
     * @return User with elasticsearch roles
     */
    @Override
    protected User doAuthenticate(UsernamePasswordToken token) {
        try (AbstractLdapConnection session = connectionFactory.open(token.principal(), token.credentials())) {
            List<String> groupDNs = session.groups();
            Set<String> roles = roleMapper.mapRoles(groupDNs);
            return new User.Simple(token.principal(), roles.toArray(new String[roles.size()]));
        } catch (ShieldException e){
            if (logger.isDebugEnabled()) {
                logger.debug("Authentication Failed for user [{}]", e, token.principal());
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
}
