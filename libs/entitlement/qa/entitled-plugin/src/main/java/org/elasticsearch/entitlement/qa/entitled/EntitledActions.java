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
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.security.SecureRandom;

@SuppressForbidden(reason = "Exposes forbidden APIs for testing purposes")
public final class EntitledActions {
    private EntitledActions() {}

    private static final SecureRandom random = new SecureRandom();

    private static final Path testRootDir = Paths.get(System.getProperty("es.entitlements.testdir"));

    private static Path readDir() {
        return testRootDir.resolve("read_dir");
    }

    private static Path readWriteDir() {
        return testRootDir.resolve("read_write_dir");
    }

    public static UserPrincipal getFileOwner(Path path) throws IOException {
        return Files.getOwner(path);
    }

    public static void createFile(Path path) throws IOException {
        Files.createFile(path);
    }

    public static Path createTempFileForRead() throws IOException {
        return Files.createFile(readDir().resolve("entitlements-" + random.nextLong() + ".tmp"));
    }

    public static Path createTempFileForWrite() throws IOException {
        return Files.createFile(readWriteDir().resolve("entitlements-" + random.nextLong() + ".tmp"));
    }

    public static Path createTempDirectoryForWrite() throws IOException {
        return Files.createDirectory(readWriteDir().resolve("entitlements-dir-" + random.nextLong()));
    }

    public static Path createTempSymbolicLink() throws IOException {
        return Files.createSymbolicLink(readDir().resolve("entitlements-link-" + random.nextLong()), readWriteDir());
    }
}
