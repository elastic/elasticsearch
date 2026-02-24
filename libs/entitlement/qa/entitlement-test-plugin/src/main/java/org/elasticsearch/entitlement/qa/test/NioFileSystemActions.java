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
import java.nio.file.FileSystemException;
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

@SuppressWarnings({ "unused" /* called via reflection */ })
class NioFileSystemActions {

    @EntitlementTest(expectedAccess = SERVER_ONLY)
    static void createFileSystemProvider() {
        new DummyImplementations.DummyFileSystemProvider();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void checkNewFileSystemFromUri() throws IOException {
        try (var fs = FileSystems.getDefault().provider().newFileSystem(URI.create("/dummy/path"), Map.of())) {}
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedExceptionIfDenied = IOException.class)
    static void checkNewFileSystemFromPath() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var newFs = fs.newFileSystem(Path.of("/dummy/path"), Map.of())) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewInputStream() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var is = fs.newInputStream(FileCheckActions.readFile())) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewOutputStream() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var os = fs.newOutputStream(FileCheckActions.readWriteFile())) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewFileChannelRead() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var fc = fs.newFileChannel(FileCheckActions.readFile(), Set.of(StandardOpenOption.READ))) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewFileChannelWrite() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var fc = fs.newFileChannel(FileCheckActions.readWriteFile(), Set.of(StandardOpenOption.WRITE))) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewAsynchronousFileChannel() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (
            var fc = fs.newAsynchronousFileChannel(
                FileCheckActions.readWriteFile(),
                Set.of(StandardOpenOption.WRITE),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            )
        ) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewByteChannel() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var bc = fs.newByteChannel(FileCheckActions.readWriteFile(), Set.of(StandardOpenOption.WRITE))) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkNewDirectoryStream() throws IOException {
        var fs = FileSystems.getDefault().provider();
        try (var bc = fs.newDirectoryStream(FileCheckActions.readDir(), entry -> false)) {}
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkCreateDirectory() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectoryForWrite();
        fs.createDirectory(directory.resolve("subdir"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkCreateSymbolicLink() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectoryForWrite();
        try {
            fs.createSymbolicLink(directory.resolve("link"), FileCheckActions.readFile());
        } catch (UnsupportedOperationException | FileSystemException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkCreateLink() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectoryForWrite();
        try {
            fs.createLink(directory.resolve("link"), FileCheckActions.readFile());
        } catch (UnsupportedOperationException | FileSystemException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkDelete() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFileForWrite();
        fs.delete(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkDeleteIfExists() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFileForWrite();
        fs.deleteIfExists(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkReadSymbolicLink() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var link = EntitledActions.createTempSymbolicLink();
        fs.readSymbolicLink(link);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkCopy() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectoryForWrite();
        fs.copy(FileCheckActions.readFile(), directory.resolve("copied"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkMove() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var directory = EntitledActions.createTempDirectoryForWrite();
        var file = EntitledActions.createTempFileForWrite();
        fs.move(file, directory.resolve("moved"));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkIsSameFile() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.isSameFile(FileCheckActions.readWriteFile(), FileCheckActions.readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkIsHidden() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.isHidden(FileCheckActions.readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkGetFileStore() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFileForRead();
        var store = fs.getFileStore(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkCheckAccess() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.checkAccess(FileCheckActions.readFile());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED, expectedDefaultIfDenied = "null")
    static String checkGetFileAttributeView() {
        var fs = FileSystems.getDefault().provider();
        return String.valueOf(fs.getFileAttributeView(FileCheckActions.readFile(), FileOwnerAttributeView.class));
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkReadAttributesWithClass() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.readAttributes(FileCheckActions.readFile(), BasicFileAttributes.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkReadAttributesWithString() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.readAttributes(FileCheckActions.readFile(), "*");
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkReadAttributesIfExists() throws IOException {
        var fs = FileSystems.getDefault().provider();
        fs.readAttributesIfExists(FileCheckActions.readFile(), BasicFileAttributes.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedExceptionIfDenied = IOException.class)
    static void checkSetAttribute() throws IOException {
        var fs = FileSystems.getDefault().provider();
        var file = EntitledActions.createTempFileForWrite();
        try {
            fs.setAttribute(file, "dos:hidden", true);
        } catch (UnsupportedOperationException | IllegalArgumentException | FileSystemException e) {
            // OK if the file does not have/does not support the attribute
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS, expectedDefaultIfDenied = "false")
    static String checkExists() {
        var fs = FileSystems.getDefault().provider();
        return String.valueOf(fs.exists(FileCheckActions.readFile()));
    }

    private NioFileSystemActions() {}
}
