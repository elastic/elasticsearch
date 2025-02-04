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

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Describes entitlement to access files at a particular location.
 *
 * @param path the location of the files. Will be automatically {@link #normalizePath normalized}.
 *            For directories, implicitly includes access to all contained files and (recursively) subdirectories.
 * @param mode the type of operation
 */
public record FileEntitlement(String path, Mode mode) implements Entitlement {

    public enum Mode {
        READ,
        READ_WRITE
    }

    public FileEntitlement {
        path = normalizePath(Paths.get(path));
    }

    /**
     * @return the "canonical" form of of the given {@code path}, to be used for entitlement checks.
     */
    public static String normalizePath(Path path) {
        return path.toAbsolutePath().normalize().toString().replace('\\', '/');
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
