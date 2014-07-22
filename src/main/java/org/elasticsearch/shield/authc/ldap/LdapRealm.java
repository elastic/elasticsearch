/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.AuthenticationToken;
import org.elasticsearch.shield.SecurityException;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.transport.TransportMessage;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends CachingUsernamePasswordRealm implements Realm<UsernamePasswordToken> {

    private static final String TYPE = "ldap";

    private final LdapConnectionFactory connectionFactory;
    private final LdapGroupToRoleMapper roleMapper;

    @Inject
    public LdapRealm(Settings settings, LdapConnectionFactory ldap, LdapGroupToRoleMapper roleMapper) {
        super(settings);

        this.connectionFactory = ldap;
        this.roleMapper = roleMapper;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public UsernamePasswordToken token(TransportMessage<?> message) {
        return UsernamePasswordToken.extractToken(message, null);
    }

    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    /**
     * Given a username and password, connect to ldap, retrieve groups, map to roles and build the user.
     * @return User with elasticsearch roles
     */
    @Override
    protected User doAuthenticate(UsernamePasswordToken token) {
        try (LdapConnection session = connectionFactory.bind(token.principal(), token.credentials())) {
            List<String> groupDNs = session.getGroups();
            Set<String> roles = roleMapper.mapRoles(groupDNs);
            User.Simple user = new User.Simple(token.principal(), roles.toArray(new String[roles.size()]));
            Arrays.fill(token.credentials(), '\0');
            return user;
        } catch (SecurityException e){
            logger.info("Authentication Failed for user [{}]", e, token.principal());
            return null;
        }
    }

}
