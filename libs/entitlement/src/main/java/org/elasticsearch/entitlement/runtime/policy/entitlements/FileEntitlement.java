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

/**
 * Describes entitlement to access files at a particular location.
 *
 * @param path the location of the files. For directories, implicitly includes access to
 *            all contained files and (recursively) subdirectories.
 * @param mode the type of operation
 */
public record FileEntitlement(String path, Mode mode) implements Entitlement {

    public enum Mode {
        READ,
        READ_WRITE
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
    public static FileEntitlement create(String path, String mode) {
        return new FileEntitlement(path, parseMode(mode));
    }
}
