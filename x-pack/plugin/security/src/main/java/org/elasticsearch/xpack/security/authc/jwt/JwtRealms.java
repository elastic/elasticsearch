/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmsSettings;
import org.elasticsearch.xpack.security.authc.InternalRealms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Common settings shared by all JwtRealm instances.
 * @see JwtRealm
 */
public class JwtRealms {

    private final List<String> principalClaimNames;
    private final List<JwtRealm> jwtRealms = new ArrayList<>();

    /**
     * Parse all xpack settings passed in from {@link InternalRealms#getFactories}
     * @param settings All xpack settings
     */
    public JwtRealms(final Settings settings) {
        this.principalClaimNames = Collections.unmodifiableList(JwtRealmsSettings.PRINCIPAL_CLAIMS_SETTING.get(settings));
    }

    /**
     * Return prioritized list of principal claim names to use for computing realm cache keys for all JWT realms.
     * @return Prioritized list of principal claim names (ex: sub, oid, client_id, azp, appid, client_id, email).
     */
    public List<String> getPrincipalClaimNames() {
        return this.principalClaimNames;
    }

    /**
     * Register a JWT realm.
     * @param jwtRealm JWT realm to be registered.
     */
    public void addRegisteredJwtRealm(final JwtRealm jwtRealm) {
        this.jwtRealms.add(jwtRealm);
    }

    /**
     * Unregister a JWT realm.
     * @param jwtRealm JWT realm to be unregistered.
     */
    public void removeRegisteredJwtRealm(final JwtRealm jwtRealm) {
        this.jwtRealms.remove(jwtRealm);
    }

    /**
     * Unregister all JWT realms.
     */
    public void clearRegisteredJwtRealms() {
        this.jwtRealms.clear();
    }

    /**
     * List all register JWT realms.
     */
    public List<JwtRealm> listRegisteredJwtRealms() {
        return Collections.unmodifiableList(this.jwtRealms);
    }
}
