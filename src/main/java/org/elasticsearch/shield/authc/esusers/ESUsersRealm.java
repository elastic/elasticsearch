/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.name.Named;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.support.UserPasswdStore;
import org.elasticsearch.shield.authc.support.UserRolesStore;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class ESUsersRealm extends AbstractComponent implements Realm<UsernamePasswordToken> {

    public static final String TYPE = "esusers";

    final UserPasswdStore userPasswdStore;
    final UserRolesStore userRolesStore;

    @Inject
    public ESUsersRealm(Settings settings, @Named("file") UserPasswdStore userPasswdStore, @Named("file") UserRolesStore userRolesStore) {
        super(settings);
        this.userPasswdStore = userPasswdStore;
        this.userRolesStore = userRolesStore;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public UsernamePasswordToken token(TransportRequest request) {
        return UsernamePasswordToken.extractToken(request, null);
    }

    @Override
    public User authenticate(UsernamePasswordToken token) {
        if (userPasswdStore == null) {
            return null;
        }
        if (!userPasswdStore.verifyPassword(token.principal(), token.credentials())) {
            return null;
        }
        String[] roles = Strings.EMPTY_ARRAY;
        if (userRolesStore != null) {
            roles = userRolesStore.roles(token.principal());
        }
        return new User.Simple(token.principal(), roles);
    }
}
