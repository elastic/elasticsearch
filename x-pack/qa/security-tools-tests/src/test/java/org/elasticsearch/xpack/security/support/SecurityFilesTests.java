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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
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

        final Map<String, String> map = new TreeMap<>();
        map.put("This is the first", "line");
        map.put("This is the second", "line");
        SecurityFiles.writeFileAtomically(path, map, e -> e.getKey() + " " + e.getValue());

        final List<String> lines = Files.readAllLines(path);
        assertThat(lines, hasSize(2));
        assertThat(lines.get(0), equalTo("This is the first line"));
        assertThat(lines.get(1), equalTo("This is the second line"));

        Set<PosixFilePermission> permissionsAfterWrite = Files.getPosixFilePermissions(path);
        assertThat(permissionsAfterWrite, is(perms));
    }

    public void testFailure() throws IOException {
        final Path path = createTempFile("existing", "file");

        Files.write(path, "foo".getBytes(StandardCharsets.UTF_8));

        final Visitor innerVisitor = new Visitor(path);
        final RuntimeException re = expectThrows(RuntimeException.class, () -> SecurityFiles.writeFileAtomically(
                path,
                Collections.singletonMap("foo", "bar"),
                e -> {
                    try {
                        Files.walkFileTree(path.getParent(), innerVisitor);
                    } catch (final IOException inner) {
                        throw new UncheckedIOException(inner);
                    }
                    throw new RuntimeException(e.getKey() + " " + e.getValue());
                }
        ));

        assertThat(re, hasToString(containsString("foo bar")));

        // assert the temporary file was created while trying to write the file
        assertTrue(innerVisitor.found());

        final Visitor visitor = new Visitor(path);
        Files.walkFileTree(path.getParent(), visitor);

        // now assert the temporary file was cleaned up after the write failed
        assertFalse(visitor.found());

        // finally, assert the original file contents remain
        final List<String> lines = Files.readAllLines(path);
        assertThat(lines, hasSize(1));
        assertThat(lines.get(0), equalTo("foo"));
    }

    static final class Visitor extends SimpleFileVisitor<Path> {

        private final Path path;
        private final AtomicBoolean found = new AtomicBoolean();

        Visitor(final Path path) {
            this.path = path;
        }

        public boolean found() {
            return found.get();
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (file.getFileName().toString().startsWith(path.getFileName().toString()) && file.getFileName().toString().endsWith("tmp")) {
                found.set(true);
                return FileVisitResult.TERMINATE;
            }
            return FileVisitResult.CONTINUE;
        }
    }

    public void testThatOwnerAndGroupAreChanged() throws Exception {
        Configuration jimFsConfiguration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        try (FileSystem fs = Jimfs.newFileSystem(jimFsConfiguration)) {
            Path path = fs.getPath("foo");
            Path tempPath = fs.getPath("bar");
            Files.write(path, "foo".getBytes(StandardCharsets.UTF_8));
            Files.write(tempPath, "bar".getBytes(StandardCharsets.UTF_8));

            PosixFileAttributeView view = Files.getFileAttributeView(path, PosixFileAttributeView.class);
            view.setGroup(fs.getUserPrincipalLookupService().lookupPrincipalByGroupName(randomAlphaOfLength(10)));
            view.setOwner(fs.getUserPrincipalLookupService().lookupPrincipalByName(randomAlphaOfLength(10)));

            PosixFileAttributeView tempPathView = Files.getFileAttributeView(tempPath, PosixFileAttributeView.class);
            assertThat(tempPathView.getOwner(), not(equalTo(view.getOwner())));
            assertThat(tempPathView.readAttributes().group(), not(equalTo(view.readAttributes().group())));

            SecurityFiles.setPosixAttributesOnTempFile(path, tempPath);

            assertThat(tempPathView.getOwner(), equalTo(view.getOwner()));
            assertThat(tempPathView.readAttributes().group(), equalTo(view.readAttributes().group()));
        }
    }
}
