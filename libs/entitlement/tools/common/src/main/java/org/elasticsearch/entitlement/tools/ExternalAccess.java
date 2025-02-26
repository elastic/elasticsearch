/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.stream.Collectors;

public enum ExternalAccess {
    PUBLIC_CLASS,
    PUBLIC_METHOD,
    PROTECTED_METHOD;

    private static final String DELIMITER = ":";

    public static String toString(EnumSet<ExternalAccess> externalAccesses) {
        return externalAccesses.stream().map(Enum::toString).collect(Collectors.joining(DELIMITER));
    }

    public static EnumSet<ExternalAccess> fromPermissions(
        boolean packageExported,
        boolean publicClass,
        boolean publicMethod,
        boolean protectedMethod
    ) {
        if (publicMethod && protectedMethod) {
            throw new IllegalArgumentException();
        }

        EnumSet<ExternalAccess> externalAccesses = EnumSet.noneOf(ExternalAccess.class);
        if (publicMethod) {
            externalAccesses.add(ExternalAccess.PUBLIC_METHOD);
        } else if (protectedMethod) {
            externalAccesses.add(ExternalAccess.PROTECTED_METHOD);
        }

        if (packageExported && publicClass) {
            externalAccesses.add(ExternalAccess.PUBLIC_CLASS);
        }
        return externalAccesses;
    }

    public static boolean isExternallyAccessible(EnumSet<ExternalAccess> access) {
        return access.contains(ExternalAccess.PUBLIC_CLASS)
            && (access.contains(ExternalAccess.PUBLIC_METHOD) || access.contains(ExternalAccess.PROTECTED_METHOD));
    }

    public static EnumSet<ExternalAccess> fromString(String accessAsString) {
        if ("PUBLIC".equals(accessAsString)) {
            return EnumSet.of(ExternalAccess.PUBLIC_CLASS, ExternalAccess.PUBLIC_METHOD);
        }
        if ("PUBLIC-METHOD".equals(accessAsString)) {
            return EnumSet.of(ExternalAccess.PUBLIC_METHOD);
        }
        if ("PRIVATE".equals(accessAsString)) {
            return EnumSet.noneOf(ExternalAccess.class);
        }

        return EnumSet.copyOf(Arrays.stream(accessAsString.split(DELIMITER)).map(ExternalAccess::valueOf).toList());
    }
}
