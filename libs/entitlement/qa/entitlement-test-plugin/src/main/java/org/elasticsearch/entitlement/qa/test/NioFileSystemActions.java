/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.SERVER_ONLY;

class NioFileSystemActions {

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void createFileSystemProvider() {
        new DummyImplementations.DummyFileSystemProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void checkNewFileSystemFromUri() throws IOException {
        try (var fs = FileSystems.getDefault().provider().newFileSystem(URI.create("/dummy/path"), Map.of())) {}
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void checkNewFileSystemFromPath() {
        var fs = FileSystems.getDefault().provider();
        try (var newFs = fs.newFileSystem(Path.of("/dummy/path"), Map.of())) {} catch (IOException e) {
            // When entitled, we expect to throw IOException, as the path is not valid - we don't really want to create a FS
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewInputStream() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try (var is = fs.newInputStream(file)) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewOutputStream() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try (var os = fs.newOutputStream(file)) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewFileChannelRead() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try (var fc = fs.newFileChannel(file, Set.of(StandardOpenOption.READ))) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewFileChannelWrite() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try (var fc = fs.newFileChannel(file, Set.of(StandardOpenOption.WRITE))) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewAsynchronousFileChannel() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try (var fc = fs.newAsynchronousFileChannel(file, Set.of(StandardOpenOption.WRITE), EsExecutors.DIRECT_EXECUTOR_SERVICE)) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewByteChannel() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try (var bc = fs.newByteChannel(file, Set.of(StandardOpenOption.WRITE))) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkNewDirectoryStream() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectory();
        try (var bc = fs.newDirectoryStream(directory, entry -> false)) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkCreateDirectory() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectory();
        fs.createDirectory(directory.resolve("subdir"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkCreateSymbolicLink() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectory();
        var file = EntitledActions.createTempFile();
        try {
            fs.createSymbolicLink(directory.resolve("link"), file);
        } catch (UnsupportedOperationException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkCreateLink() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectory();
        var file = EntitledActions.createTempFile();
        try {
            fs.createLink(directory.resolve("link"), file);
        } catch (UnsupportedOperationException e) {
            // OK not to implement symbolic link in the filesystem
        }

    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkDelete() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.delete(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkDeleteIfExists() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.deleteIfExists(Path.of("/dummy/path"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkReadSymbolicLink() {
        // TODO
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkCopy() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectory();
        var file = EntitledActions.createTempFile();
        fs.copy(file, directory.resolve("copied"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkMove() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectory();
        var file = EntitledActions.createTempFile();
        fs.move(file, directory.resolve("moved"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkIsSameFile() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file1 = EntitledActions.createTempFile();
        var file2 = EntitledActions.createTempFile();
        fs.isSameFile(file1, file2);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkIsHidden() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.isHidden(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkGetFileStore() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        var store = fs.getFileStore(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkCheckAccess() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.checkAccess(file);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void checkGetFileAttributeView() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.getFileAttributeView(file, FileOwnerAttributeView.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkReadAttributesWithClass() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.readAttributes(file, BasicFileAttributes.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkReadAttributesWithString() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.readAttributes(file, "*");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkReadAttributesIfExists() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        fs.readAttributesIfExists(file, BasicFileAttributes.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkSetAttribute() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFile();
        try {
            fs.setAttribute(file, "dos:hidden", true);
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
            // OK if the file does not have/does not support the attribute
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkExists() {
        var fs = FileSystems.getDefault().provider();
        fs.exists(Path.of("/dummy/path"));
    }

    private NioFileSystemActions() {}
}
