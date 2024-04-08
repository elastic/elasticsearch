/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public class FileUtilsTests extends ESTestCase {

    public void test_recreateTempDirectoryIfNeeded_forWindows() throws IOException {
        FileSystem fileSystem = Jimfs.newFileSystem(Configuration.windows());
        PathUtilsForTesting.installMock(fileSystem);

        Path tmpDir = fileSystem.getPath("c:\\tmp\\elasticsearch");

        assertFalse(Files.exists(tmpDir));
        FileUtils.recreateTempDirectoryIfNeeded(tmpDir);
        assertTrue(Files.exists(tmpDir));

        BasicFileAttributes attributes = Files.readAttributes(tmpDir, BasicFileAttributes.class);
        assertTrue(attributes.isDirectory());
    }

    public void test_recreateTempDirectoryIfNeeded_forPosix() throws IOException {
        FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix().toBuilder().setAttributeViews("posix").build());
        PathUtilsForTesting.installMock(fileSystem);

        Path tmpDir = fileSystem.getPath("/tmp/elasticsearch-1234567890");

        assertFalse(Files.exists(tmpDir));
        FileUtils.recreateTempDirectoryIfNeeded(tmpDir);
        assertTrue(Files.exists(tmpDir));

        PosixFileAttributes attributes = Files.readAttributes(tmpDir, PosixFileAttributes.class);
        assertTrue(attributes.isDirectory());
        assertEquals(
            attributes.permissions(),
            Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE)
        );
    }
}
