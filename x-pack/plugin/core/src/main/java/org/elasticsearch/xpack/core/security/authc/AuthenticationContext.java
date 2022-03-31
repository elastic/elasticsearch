/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import static org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;

public class AuthenticationContext {

    private final Version version;
    private final Subject authenticatingSubject;
    private final Subject effectiveSubject;
    // TODO: Rename to AuthenticationMethod
    private final AuthenticationType type;

    private AuthenticationContext(
        Version version,
        Subject authenticatingSubject,
        Subject effectiveSubject,
        AuthenticationType authenticationType
    ) {
        this.version = version;
        this.authenticatingSubject = authenticatingSubject;
        this.effectiveSubject = effectiveSubject;
        this.type = authenticationType;
    }

    public boolean isRunAs() {
        assert authenticatingSubject != null && effectiveSubject != null;
        return authenticatingSubject != effectiveSubject;
    }

    public Subject getAuthenticatingSubject() {
        return authenticatingSubject;
    }

    public Subject getEffectiveSubject() {
        return effectiveSubject;
    }

    public static AuthenticationContext fromAuthentication(Authentication authentication) {
        final Builder builder = new Builder(authentication.getVersion());
        builder.authenticationType(authentication.getAuthenticationType());
        final User user = authentication.getUser();
        if (user.isRunAs()) {
            builder.authenticatingSubject(user.authenticatedUser(), authentication.getAuthenticatedBy(), authentication.getMetadata());
            // The lookup user for run-as currently don't have authentication metadata associated with them because
            // lookupUser only returns the User object. The lookup user for authorization delegation does have
            // authentication metadata, but the realm does not expose this difference between authenticatingUser and
            // delegateUser so effectively this is handled together with the authenticatingSubject not effectiveSubject.
            builder.effectiveSubject(user, authentication.getLookedUpBy(), Map.of());
        } else {
            builder.authenticatingSubject(user, authentication.getAuthenticatedBy(), authentication.getMetadata());
        }
        return builder.build();
    }

    public static class Builder {
        private final Version version;
        private AuthenticationType authenticationType;
        private Subject authenticatingSubject;
        private Subject effectiveSubject;

        public Builder() {
            this(Version.CURRENT);
        }

        public Builder(Version version) {
            this.version = version;
        }

        public Builder authenticationType(AuthenticationType authenticationType) {
            this.authenticationType = authenticationType;
            return this;
        }

        public Builder authenticatingSubject(User authenticatingUser, RealmRef authenticatingRealmRef, Map<String, Object> metadata) {
            this.authenticatingSubject = new Subject(authenticatingUser, authenticatingRealmRef, version, metadata);
            return this;
        }

        public Builder effectiveSubject(User effectiveUser, RealmRef lookupRealmRef, Map<String, Object> metadata) {
            this.effectiveSubject = new Subject(effectiveUser, lookupRealmRef, version, metadata);
            return this;
        }

        public AuthenticationContext build() {
            if (effectiveSubject == null) {
                effectiveSubject = authenticatingSubject;
            }
            return new AuthenticationContext(version, authenticatingSubject, effectiveSubject, authenticationType);
        }
    }

}
