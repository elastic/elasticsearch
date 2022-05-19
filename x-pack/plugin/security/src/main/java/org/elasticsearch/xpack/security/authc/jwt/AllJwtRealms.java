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
public class AllJwtRealms {

    private final List<String> principalClaimNames;
    private final List<JwtRealm> allJwtRealms = new ArrayList<>();

    /**
     * Parse all realm settings passed in from {@link InternalRealms#getFactories}
     * @param settings All xpack settings
     */
    public AllJwtRealms(final Settings settings) {
        this(JwtRealmsSettings.PRINCIPAL_CLAIMS_SETTING.get(settings));
    }

    // Package protected for testing
    // If multiple settings are needed in future, change method signature to accept Settings.
    AllJwtRealms(final List<String> principalClaimNames) {
        this.principalClaimNames = Collections.unmodifiableList(principalClaimNames);
    }

    public List<String> getPrincipalClaimNames() {
        return this.principalClaimNames;
    }

    public List<JwtRealm> list() {
        return Collections.unmodifiableList(this.allJwtRealms);
    }

    public void add(JwtRealm jwtRealm) {
        this.allJwtRealms.add(jwtRealm);
    }
}
