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

    public List<String> getPrincipalClaimNames() {
        return this.principalClaimNames;
    }

    public void addRegisteredJwtRealm(final JwtRealm jwtRealm) {
        this.jwtRealms.add(jwtRealm);
    }

    public void removeRegisteredJwtRealm(final JwtRealm jwtRealm) {
        this.jwtRealms.remove(jwtRealm);
    }

    public void clearRegisteredJwtRealms(final JwtRealm jwtRealm) {
        this.jwtRealms.clear();
    }

    public List<JwtRealm> listRegisteredJwtRealms() {
        return Collections.unmodifiableList(this.jwtRealms);
    }
}
