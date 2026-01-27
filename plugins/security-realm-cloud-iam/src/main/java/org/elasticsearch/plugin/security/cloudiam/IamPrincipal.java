/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import java.util.Objects;

public final class IamPrincipal {
    public enum PrincipalType {
        USER,
        ROLE,
        ASSUMED_ROLE,
        UNKNOWN
    }

    private final String arn;
    private final String accountId;
    private final String userId;
    private final PrincipalType principalType;

    public IamPrincipal(String arn, String accountId, String userId, PrincipalType principalType) {
        this.arn = Objects.requireNonNull(arn);
        this.accountId = Objects.requireNonNull(accountId);
        this.userId = userId;
        this.principalType = Objects.requireNonNull(principalType);
    }

    public String arn() {
        return arn;
    }

    public String accountId() {
        return accountId;
    }

    public String userId() {
        return userId;
    }

    public PrincipalType principalType() {
        return principalType;
    }

    public static PrincipalType principalTypeFromArn(String arn) {
        if (arn == null) {
            return PrincipalType.UNKNOWN;
        }
        // Aliyun ARN format: acs:service:region:account-id:resource
        // AWS ARN format: arn:partition:service:region:account-id:resource
        // Both formats have the resource as the last colon-separated part
        int lastColonIdx = arn.lastIndexOf(':');
        if (lastColonIdx < 0) {
            return PrincipalType.UNKNOWN;
        }
        String resource = arn.substring(lastColonIdx + 1);
        if (resource.startsWith("user/")) {
            return PrincipalType.USER;
        }
        if (resource.startsWith("role/")) {
            return PrincipalType.ROLE;
        }
        if (resource.startsWith("assumed-role/")) {
            return PrincipalType.ASSUMED_ROLE;
        }
        return PrincipalType.UNKNOWN;
    }
}
