/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.util.HashSet;
import java.util.Set;

public class FileAttributesCheckerTests extends ESTestCase {

    public void testNonExistentFile() throws Exception {
        Path path = createTempDir().resolve("dne");
        FileAttributesChecker checker = new FileAttributesChecker(path);
        MockTerminal terminal = MockTerminal.create();
        checker.check(terminal);
        assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
        assertTrue(terminal.getErrorOutput(), terminal.getErrorOutput().isEmpty());
    }

    public void testNoPosix() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("basic").build();
        try (FileSystem fs = Jimfs.newFileSystem(conf)) {
            Path path = fs.getPath("temp");
            FileAttributesChecker checker = new FileAttributesChecker(path);
            MockTerminal terminal = MockTerminal.create();
            checker.check(terminal);
            assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
            assertTrue(terminal.getErrorOutput(), terminal.getErrorOutput().isEmpty());
        }
    }

    public void testNoChanges() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        try (FileSystem fs = Jimfs.newFileSystem(conf)) {
            Path path = fs.getPath("temp");
            Files.createFile(path);
            FileAttributesChecker checker = new FileAttributesChecker(path);

            MockTerminal terminal = MockTerminal.create();
            checker.check(terminal);
            assertTrue(terminal.getOutput(), terminal.getOutput().isEmpty());
            assertTrue(terminal.getErrorOutput(), terminal.getErrorOutput().isEmpty());
        }
    }

    public void testPermissionsChanged() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        try (FileSystem fs = Jimfs.newFileSystem(conf)) {
            Path path = fs.getPath("temp");
            Files.createFile(path);

            PosixFileAttributeView attrs = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            Set<PosixFilePermission> perms = new HashSet<>(attrs.readAttributes().permissions());
            perms.remove(PosixFilePermission.GROUP_READ);
            attrs.setPermissions(perms);

            FileAttributesChecker checker = new FileAttributesChecker(path);
            perms.add(PosixFilePermission.GROUP_READ);
            attrs.setPermissions(perms);

            MockTerminal terminal = MockTerminal.create();
            checker.check(terminal);
            String output = terminal.getErrorOutput();
            assertTrue(output, output.contains("permissions of [" + path + "] have changed"));
        }
    }

    public void testOwnerChanged() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        try (FileSystem fs = Jimfs.newFileSystem(conf)) {
            Path path = fs.getPath("temp");
            Files.createFile(path);
            FileAttributesChecker checker = new FileAttributesChecker(path);

            UserPrincipal newOwner = fs.getUserPrincipalLookupService().lookupPrincipalByName("randomuser");
            PosixFileAttributeView attrs = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            attrs.setOwner(newOwner);

            MockTerminal terminal = MockTerminal.create();
            checker.check(terminal);
            String output = terminal.getErrorOutput();
            assertTrue(output, output.contains("Owner of file [" + path + "] used to be"));
        }
    }

    public void testGroupChanged() throws Exception {
        Configuration conf = Configuration.unix().toBuilder().setAttributeViews("posix").build();
        try (FileSystem fs = Jimfs.newFileSystem(conf)) {
            Path path = fs.getPath("temp");
            Files.createFile(path);
            FileAttributesChecker checker = new FileAttributesChecker(path);

            GroupPrincipal newGroup = fs.getUserPrincipalLookupService().lookupPrincipalByGroupName("randomgroup");
            PosixFileAttributeView attrs = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            attrs.setGroup(newGroup);

            MockTerminal terminal = MockTerminal.create();
            checker.check(terminal);
            String output = terminal.getErrorOutput();
            assertTrue(output, output.contains("Group of file [" + path + "] used to be"));
        }
    }
}
