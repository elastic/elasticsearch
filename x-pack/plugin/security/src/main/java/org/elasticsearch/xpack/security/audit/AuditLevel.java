/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;

public enum AuditLevel {

    
    ANONYMOUS_ACCESS_DENIED,
    AUTHENTICATION_FAILED,
    REALM_AUTHENTICATION_FAILED,
    ACCESS_GRANTED,
    ACCESS_DENIED,
    TAMPERED_REQUEST,
    CONNECTION_GRANTED,
    CONNECTION_DENIED,
    SYSTEM_ACCESS_GRANTED,
    AUTHENTICATION_SUCCESS,
    RUN_AS_GRANTED,
    RUN_AS_DENIED;

    static EnumSet<AuditLevel> parse(List<String> levels) {
        EnumSet<AuditLevel> enumSet = EnumSet.noneOf(AuditLevel.class);
        for (String level : levels) {
            String lowerCaseLevel = level.trim().toLowerCase(Locale.ROOT);
            switch (lowerCaseLevel) {
                case "_all":
                    enumSet.addAll(Arrays.asList(AuditLevel.values()));
                    break;
                case "anonymous_access_denied":
                    enumSet.add(ANONYMOUS_ACCESS_DENIED);
                    break;
                case "authentication_failed":
                    enumSet.add(AUTHENTICATION_FAILED);
                    break;
                case "realm_authentication_failed":
                    enumSet.add(REALM_AUTHENTICATION_FAILED);
                    break;
                case "access_granted":
                    enumSet.add(ACCESS_GRANTED);
                    break;
                case "access_denied":
                    enumSet.add(ACCESS_DENIED);
                    break;
                case "tampered_request":
                    enumSet.add(TAMPERED_REQUEST);
                    break;
                case "connection_granted":
                    enumSet.add(CONNECTION_GRANTED);
                    break;
                case "connection_denied":
                    enumSet.add(CONNECTION_DENIED);
                    break;
                case "system_access_granted":
                    enumSet.add(SYSTEM_ACCESS_GRANTED);
                    break;
                case "authentication_success":
                    enumSet.add(AUTHENTICATION_SUCCESS);
                    break;
                case "run_as_granted":
                    enumSet.add(RUN_AS_GRANTED);
                    break;
                case "run_as_denied":
                    enumSet.add(RUN_AS_DENIED);
                    break;
                default:
                    throw new IllegalArgumentException("invalid event name specified [" + level + "]");
            }
        }
        return enumSet;
    }

    public static EnumSet<AuditLevel> parse(List<String> includeLevels, List<String> excludeLevels) {
        EnumSet<AuditLevel> included = parse(includeLevels);
        EnumSet<AuditLevel> excluded = parse(excludeLevels);
        included.removeAll(excluded);
        return included;
    }
}
