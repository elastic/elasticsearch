/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Map;

public record Profile(
    String uid,
    boolean enabled,
    long lastSynchronized,
    ProfileUser user,
    Access access,
    Map<String, Object> applicationData,
    VersionControl versionControl
) {

    public record QualifiedName(String username, String realmDomain) {}

    public record ProfileUser(
        String username,
        String realmName,
        @Nullable String realmDomain,
        String email,
        String fullName,
        String displayName
    ) {
        public QualifiedName qualifiedName() {
            return new QualifiedName(username, realmDomain);
        }
    }

    public record Access(List<String> roles, Map<String, Object> applications) {}

    public record VersionControl(long primaryTerm, long seqNo) {}
}
