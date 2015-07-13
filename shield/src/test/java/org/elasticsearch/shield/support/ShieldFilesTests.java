/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.PrintWriter;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Locale;
import java.util.Set;

import static java.nio.file.attribute.PosixFilePermission.*;
import static org.elasticsearch.shield.support.ShieldFiles.openAtomicMoveWriter;
import static org.hamcrest.Matchers.*;

public class ShieldFilesTests extends ElasticsearchTestCase {

    @Test
    public void testThatOriginalPermissionsAreKept() throws Exception {
        Path path = createTempFile();

        // no posix file permissions, nothing to test, done here
        boolean supportsPosixPermissions = Files.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
        assumeTrue("Ignoring because posix file attributes are not supported", supportsPosixPermissions);

        Files.write(path, "foo".getBytes(Charsets.UTF_8));

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

    @Test
    public void testThatOwnerAndGroupAreChanged() throws Exception {
        Configuration jimFsConfiguration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        try (FileSystem fs = Jimfs.newFileSystem(jimFsConfiguration)) {
            Path path = fs.getPath("foo");
            Path tempPath = fs.getPath("bar");
            Files.write(path, "foo".getBytes(Charsets.UTF_8));
            Files.write(tempPath, "bar".getBytes(Charsets.UTF_8));

            PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            view.setGroup(fs.getUserPrincipalLookupService().lookupPrincipalByGroupName(randomAsciiOfLength(10)));
            view.setOwner(fs.getUserPrincipalLookupService().lookupPrincipalByName(randomAsciiOfLength(10)));

            PosixFileAttributeView tempPathView = Files.getFileAttributeView(tempPath, PosixFileAttributeView.class);
            assertThat(tempPathView.getOwner(), not(equalTo(view.getOwner())));
            assertThat(tempPathView.readAttributes().group(), not(equalTo(view.readAttributes().group())));

            ShieldFiles.setPosixAttributesOnTempFile(path, tempPath);

            assertThat(tempPathView.getOwner(), equalTo(view.getOwner()));
            assertThat(tempPathView.readAttributes().group(), equalTo(view.readAttributes().group()));
        }
    }
}
