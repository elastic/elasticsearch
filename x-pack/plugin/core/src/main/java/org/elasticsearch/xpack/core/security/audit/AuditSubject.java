/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.Authentication;

/**
 * Authenticated identity available to audit customization: principal and realm.
 */
public record AuditSubject(String principal, String realmName, String realmType) {

    /**
     * Builds the subject from an authentication, or {@code null} when the effective realm is absent.
     */
    @Nullable
    public static AuditSubject from(Authentication authentication) {
        final Authentication.RealmRef realm = authentication.getEffectiveSubject().getRealm();
        if (realm == null) {
            return null;
        }
        return new AuditSubject(authentication.getEffectiveSubject().getUser().principal(), realm.getName(), realm.getType());
    }
}
