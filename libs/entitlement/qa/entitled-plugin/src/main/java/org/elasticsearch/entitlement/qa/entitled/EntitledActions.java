/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.entitled;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;

public final class EntitledActions {
    private EntitledActions() {}

    @SuppressForbidden(reason = "Exposes forbidden APIs for testing purposes")
    static void System_clearProperty(String key) {
        System.clearProperty(key);
    }

    public static UserPrincipal getFileOwner(Path path) throws IOException {
        return Files.getOwner(path);
    }

    public static void createFile(Path path) throws IOException {
        Files.createFile(path);
    }
}
