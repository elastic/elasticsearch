/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.security.profile.ProfileDocument.ProfileDocumentUser;

import java.util.Map;
import java.util.Set;

public record Profile(
    String uid,
    boolean enabled,
    long lastSynchronized,
    ProfileUser user,
    ProfileDocument.Access access,
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

        public static ProfileUser fromDocument(ProfileDocumentUser doc, @Nullable String realmDomain) {
            return new ProfileUser(doc.username(), doc.realm().getName(), realmDomain, doc.email(), doc.fullName(), doc.displayName());
        }
    }

    public record VersionControl(long primaryTerm, long seqNo) {}

    public static Profile fromDocument(
        ProfileDocument doc,
        long primaryTerm,
        long seqNo,
        @Nullable String realmDomain,
        @Nullable Set<String> dataKeys
    ) {
        final Map<String, Object> applicationData;
        if (dataKeys != null && dataKeys.isEmpty()) {
            applicationData = Map.of();
        } else {
            applicationData = XContentHelper.convertToMap(doc.applicationData(), false, XContentType.JSON, dataKeys, null).v2();
        }

        return new Profile(
            doc.uid(),
            doc.enabled(),
            doc.lastSynchronized(),
            ProfileUser.fromDocument(doc.user(), realmDomain),
            doc.access(),
            applicationData,
            new VersionControl(primaryTerm, seqNo)
        );
    }
}
