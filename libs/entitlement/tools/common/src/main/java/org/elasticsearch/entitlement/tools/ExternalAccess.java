/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools;

import java.util.EnumSet;

public enum ExternalAccess {
    CLASS,
    METHOD;

    public static String toString(EnumSet<ExternalAccess> externalAccesses) {
        if (externalAccesses.contains(ExternalAccess.METHOD) && externalAccesses.contains(ExternalAccess.CLASS)) {
            return "PUBLIC";
        } else if (externalAccesses.contains(ExternalAccess.METHOD)) {
            return "PUBLIC-METHOD";
        } else {
            return "PRIVATE";
        }
    }

    public static EnumSet<ExternalAccess> fromPermissions(boolean packageExported, boolean classPublic, boolean methodPublic) {
        EnumSet<ExternalAccess> externalAccesses = EnumSet.noneOf(ExternalAccess.class);
        if (methodPublic) {
            externalAccesses.add(ExternalAccess.METHOD);
        }
        if (packageExported && classPublic) {
            externalAccesses.add(ExternalAccess.CLASS);
        }
        return externalAccesses;
    }

    public static boolean isPublic(EnumSet<ExternalAccess> externalAccesses) {
        return externalAccesses.isEmpty() == false;
    }

    public static EnumSet<ExternalAccess> fromString(String accessAsString) {
        if ("PUBLIC".equals(accessAsString)) {
            return EnumSet.of(ExternalAccess.METHOD, ExternalAccess.CLASS);
        }
        if ("PUBLIC-METHOD".equals(accessAsString)) {
            return EnumSet.of(ExternalAccess.METHOD);
        }
        return EnumSet.noneOf(ExternalAccess.class);
    }
}
