/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.transport.TransportRequest;

/**
 *
 */
public class LdapRealm extends AbstractComponent implements Realm<UsernamePasswordToken> {

    private static final String TYPE = "ldap";

    @Inject
    public LdapRealm(Settings settings) {
        super(settings);
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
        return null;
    }
}
