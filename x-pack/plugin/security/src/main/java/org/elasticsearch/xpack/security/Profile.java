/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Profile {

    public static class QualifiedName {
        private final String username;
        private final String domain;

        public QualifiedName(String username, String domain) {
            this.username = username;
            this.domain = domain;
        }

        public String getUsername() {
            return username;
        }

        public String getDomain() {
            return domain;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QualifiedName that = (QualifiedName) o;
            return username.equals(that.username) && domain.equals(that.domain);
        }

        @Override
        public int hashCode() {
            return Objects.hash(username, domain);
        }

        @Override
        public String toString() {
            return "QualifiedName{" + "username='" + username + '\'' + ", domain='" + domain + '\'' + '}';
        }
    }

    // TODO: I'd avoid using the term User if possible
    public static class User {
        private final String username;
        private final String realmName;
        private final String realmType;
        private final String realmDomain;
        private final String email;
        private final String fullName;
        private final String displayName;

        public User(
            String username,
            String realmName,
            String realmType,
            String realmDomain,
            String email,
            String fullName,
            String displayName
        ) {
            this.username = username;
            this.realmName = realmName;
            this.realmType = realmType;
            this.realmDomain = realmDomain;
            this.email = email;
            this.fullName = fullName;
            this.displayName = displayName;
        }

        public QualifiedName qualifiedName() {
            return new QualifiedName(username, realmDomain);
        }

        public String getUsername() {
            return username;
        }

        public String getRealmName() {
            return realmName;
        }

        public String getRealmType() {
            return realmType;
        }

        public String getRealmDomain() {
            return realmDomain;
        }

        public String getEmail() {
            return email;
        }

        public String getFullName() {
            return fullName;
        }

        public String getDisplayName() {
            return displayName;
        }

        @SuppressWarnings("unchecked")
        public static User fromMap(Map<String, Object> userMap) {
            final Map<String, Object> realmMap = (Map<String, Object>) userMap.get("realm");
            return new Profile.User(
                (String) userMap.get("username"),
                (String) realmMap.get("name"),
                (String) realmMap.get("type"),
                (String) realmMap.get("domain"),
                (String) userMap.get("email"),
                (String) userMap.get("full_name"),
                (String) userMap.get("display_name")
            );
        }
    }

    public static class Access {
        private final String[] roles;
        private final String[] kibanaSpaces;

        public Access(String[] roles, String[] kibanaSpaces) {
            this.roles = roles;
            this.kibanaSpaces = kibanaSpaces;
        }

        public String[] getRoles() {
            return roles;
        }

        public String[] getKibanaSpaces() {
            return kibanaSpaces;
        }

        @SuppressWarnings("unchecked")
        public static Access fromMap(Map<String, Object> m) {
            final List<String> roles = (List<String>) m.get("roles");
            final Map<String, Object> kibanaMap = (Map<String, Object>) m.get("kibana");
            final List<String> kibanaSpaces = (List<String>) kibanaMap.get("spaces");

            return new Access(roles.toArray(String[]::new), kibanaSpaces.toArray(String[]::new));
        }
    }

    public static class VersionControl {
        private final long primaryTerm;
        private final long seqNo;

        public VersionControl(long primaryTerm, long seqNo) {
            this.primaryTerm = primaryTerm;
            this.seqNo = seqNo;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        public long getSeqNo() {
            return seqNo;
        }
    }

    private final String uid;
    private final boolean enabled;
    private final long lastSynchronized;
    private final User user;
    private final Access access;
    private final Map<String, Object> applicationData;
    @Nullable
    private final VersionControl versionControl;

    public Profile(
        String uid,
        boolean enabled,
        long lastSynchronized,
        User user,
        Access access,
        Map<String, Object> applicationData,
        VersionControl versionControl
    ) {
        this.uid = uid;
        this.enabled = enabled;
        this.lastSynchronized = lastSynchronized;
        this.user = user;
        this.access = access;
        this.applicationData = applicationData;
        this.versionControl = versionControl;
    }

    public String getUid() {
        return uid;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getLastSynchronized() {
        return lastSynchronized;
    }

    public User getUser() {
        return user;
    }

    public Access getAccess() {
        return access;
    }

    public Map<String, Object> getApplicationData() {
        return applicationData;
    }

    public VersionControl getVersionControl() {
        return versionControl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Profile profile = (Profile) o;
        return enabled == profile.enabled
            && lastSynchronized == profile.lastSynchronized
            && uid.equals(profile.uid)
            && Objects.equals(user, profile.user)
            && Objects.equals(access, profile.access)
            && Objects.equals(applicationData, profile.applicationData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uid, enabled, lastSynchronized, user, access, applicationData);
    }

    public static Profile fromSource(Map<String, Object> source, long primaryTerm, long seqNo) {
        final String uid = (String) source.get("uid");
        final boolean enabled = (boolean) source.get("enabled");
        final long lastSynchronized = (long) source.get("last_synchronized");

        final Map<String, Object> userMap = getFieldAsMap(source, "user");
        final Map<String, Object> accessMap = getFieldAsMap(source, "access");

        return new Profile(
            uid,
            enabled,
            lastSynchronized,
            Profile.User.fromMap(userMap),
            Profile.Access.fromMap(accessMap),
            getFieldAsMap(source, "application_data"),
            new Profile.VersionControl(primaryTerm, seqNo)
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getFieldAsMap(Map<String, Object> m, String key) {
        return (Map<String, Object>) m.get(key);
    }
}
