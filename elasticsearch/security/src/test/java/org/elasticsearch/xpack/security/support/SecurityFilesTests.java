/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.Set;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.elasticsearch.xpack.security.support.SecurityFiles.openAtomicMoveWriter;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SecurityFilesTests extends ESTestCase {
    public void testThatOriginalPermissionsAreKept() throws Exception {
        assumeTrue("test cannot run with security manager enabled", System.getSecurityManager() == null);
        Path path = createTempFile();

        // no posix file permissions, nothing to test, done here
        boolean supportsPosixPermissions = Environment.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
        assumeTrue("Ignoring because posix file attributes are not supported", supportsPosixPermissions);

        Files.write(path, "foo".getBytes(StandardCharsets.UTF_8));

        Set<PosixFilePermission> perms = Sets.newHashSet(OWNER_READ, OWNER_WRITE);
        if (randomBoolean()) perms.add(OWNER_EXECUTE);
        if (randomBoolean()) perms.add(GROUP_EXECUTE);
        if (randomBoolean()) perms.add(OTHERS_EXECUTE);

        Files.setPosixFilePermissions(path, perms);

        try (PrintWriter writer = new PrintWriter(openAtomicMoveWriter(path))) {
            writer.printf(Locale.ROOT, "This is a test");
        }

        Set<PosixFilePermission> permissionsAfterWrite = Files.getPosixFilePermissions(path);
        assertThat(permissionsAfterWrite, is(perms));
    }

    public void testThatOwnerAndGroupAreChanged() throws Exception {
        Configuration jimFsConfiguration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        try (FileSystem fs = Jimfs.newFileSystem(jimFsConfiguration)) {
            Path path = fs.getPath("foo");
            Path tempPath = fs.getPath("bar");
            Files.write(path, "foo".getBytes(StandardCharsets.UTF_8));
            Files.write(tempPath, "bar".getBytes(StandardCharsets.UTF_8));

            PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            view.setGroup(fs.getUserPrincipalLookupService().lookupPrincipalByGroupName(randomAsciiOfLength(10)));
            view.setOwner(fs.getUserPrincipalLookupService().lookupPrincipalByName(randomAsciiOfLength(10)));

            PosixFileAttributeView tempPathView = Files.getFileAttributeView(tempPath, PosixFileAttributeView.class);
            assertThat(tempPathView.getOwner(), not(equalTo(view.getOwner())));
            assertThat(tempPathView.readAttributes().group(), not(equalTo(view.readAttributes().group())));

            SecurityFiles.setPosixAttributesOnTempFile(path, tempPath);

            assertThat(tempPathView.getOwner(), equalTo(view.getOwner()));
            assertThat(tempPathView.readAttributes().group(), equalTo(view.readAttributes().group()));
        }
    }
}
