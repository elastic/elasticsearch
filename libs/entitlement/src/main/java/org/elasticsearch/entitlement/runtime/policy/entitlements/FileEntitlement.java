/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.entitlements;

import org.elasticsearch.entitlement.runtime.policy.ExternalEntitlement;
import org.elasticsearch.entitlement.runtime.policy.PolicyValidationException;

import java.nio.file.Paths;

/**
 * Describes a file entitlement with a path and mode.
 */
public record FileEntitlement(String path, Mode mode) implements Entitlement {

    public enum Mode {
        READ,
        READ_WRITE
    }

    public FileEntitlement {
        path = normalizePath(path);
    }

    private static String normalizePath(String path) {
        return Paths.get(path).toAbsolutePath().normalize().toString();
    }

    private static Mode parseMode(String mode) {
        if (mode.equals("read")) {
            return Mode.READ;
        } else if (mode.equals("read_write")) {
            return Mode.READ_WRITE;
        } else {
            throw new PolicyValidationException("invalid mode: " + mode + ", valid values: [read, read_write]");
        }
    }

    @ExternalEntitlement(parameterNames = { "path", "mode" }, esModulesOnly = false)
    public FileEntitlement(String path, String mode) {
        this(path, parseMode(mode));
    }
}
