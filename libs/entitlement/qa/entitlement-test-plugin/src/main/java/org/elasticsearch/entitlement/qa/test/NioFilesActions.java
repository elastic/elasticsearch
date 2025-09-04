/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipal;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;
import static org.elasticsearch.entitlement.qa.test.FileCheckActions.readDir;
import static org.elasticsearch.entitlement.qa.test.FileCheckActions.readFile;
import static org.elasticsearch.entitlement.qa.test.FileCheckActions.readWriteDir;
import static org.elasticsearch.entitlement.qa.test.FileCheckActions.readWriteFile;

@SuppressWarnings({ "unused" /* called via reflection */ })
class NioFilesActions {

    @EntitlementTest(expectedAccess = PLUGINS)
    static void filesGetOwner() throws IOException {
        Files.getOwner(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void filesProbeContentType() throws IOException {
        Files.probeContentType(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void filesSetOwner() throws IOException {
        UserPrincipal owner = EntitledActions.getFileOwner(readWriteFile());
        Files.setOwner(readWriteFile(), owner); // set to existing owner, just trying to execute the method
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewInputStream() throws IOException {
        Files.newInputStream(readFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewOutputStream() throws IOException {
        Files.newOutputStream(readWriteFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewByteChannelRead() throws IOException {
        Files.newByteChannel(readFile(), Set.of(StandardOpenOption.READ)).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewByteChannelWrite() throws IOException {
        Files.newByteChannel(readWriteFile(), Set.of(StandardOpenOption.WRITE)).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewByteChannelReadVarargs() throws IOException {
        Files.newByteChannel(readFile(), StandardOpenOption.READ).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewByteChannelWriteVarargs() throws IOException {
        Files.newByteChannel(readWriteFile(), StandardOpenOption.WRITE).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewDirectoryStream() throws IOException {
        Files.newDirectoryStream(FileCheckActions.readDir()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewDirectoryStreamGlob() throws IOException {
        Files.newDirectoryStream(FileCheckActions.readDir(), "*").close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewDirectoryStreamFilter() throws IOException {
        Files.newDirectoryStream(FileCheckActions.readDir(), entry -> false).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateFile() throws IOException {
        Files.createFile(readWriteDir().resolve("file.txt"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateDirectory() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.createDirectory(directory.resolve("subdir"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateDirectories() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.createDirectories(directory.resolve("subdir").resolve("subsubdir"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateTempFileInDir() throws IOException {
        Files.createTempFile(readWriteDir(), "prefix", "suffix");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateTempDirectoryInDir() throws IOException {
        Files.createTempDirectory(readWriteDir(), "prefix");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateSymbolicLink() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        try {
            Files.createSymbolicLink(directory.resolve("link"), readFile());
        } catch (UnsupportedOperationException | FileSystemException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateRelativeSymbolicLink() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        try {
            Files.createSymbolicLink(directory.resolve("link"), Path.of("target"));
        } catch (UnsupportedOperationException | FileSystemException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateLink() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        try {
            Files.createLink(directory.resolve("link"), readFile());
        } catch (UnsupportedOperationException | FileSystemException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCreateRelativeLink() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        var target = directory.resolve("target");
        try {
            Files.createLink(directory.resolve("link"), Path.of("target"));
        } catch (UnsupportedOperationException | FileSystemException e) {
            // OK not to implement symbolic link in the filesystem
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesDelete() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        Files.delete(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesDeleteIfExists() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        Files.deleteIfExists(file);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadSymbolicLink() throws IOException {
        var link = EntitledActions.createTempSymbolicLink();
        Files.readSymbolicLink(link);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCopy() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.copy(readFile(), directory.resolve("copied"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesMove() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        var file = EntitledActions.createTempFileForWrite();
        Files.move(file, directory.resolve("moved"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsSameFile() throws IOException {
        Files.isSameFile(readWriteFile(), readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesMismatch() throws IOException {
        Files.mismatch(readWriteFile(), readFile());
    }

    @SuppressForbidden(reason = "testing entitlements on this API specifically")
    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsHidden() throws IOException {
        Files.isHidden(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesGetFileStore() throws IOException {
        var file = EntitledActions.createTempFileForRead();
        Files.getFileStore(file);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void checkFilesGetFileAttributeView() {
        Files.getFileAttributeView(readFile(), FileOwnerAttributeView.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadAttributesWithClass() throws IOException {
        Files.readAttributes(readFile(), BasicFileAttributes.class);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadAttributesWithString() throws IOException {
        Files.readAttributes(readFile(), "*");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesGetAttribute() throws IOException {
        try {
            Files.getAttribute(readFile(), "dos:hidden");
        } catch (UnsupportedOperationException | IllegalArgumentException | FileSystemException e) {
            // OK if the file does not have/does not support the attribute
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesSetAttribute() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        try {
            Files.setAttribute(file, "dos:hidden", true);
        } catch (UnsupportedOperationException | IllegalArgumentException | FileSystemException e) {
            // OK if the file does not have/does not support the attribute
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesGetPosixFilePermissions() throws IOException {
        try {
            Files.getPosixFilePermissions(readFile());
        } catch (UnsupportedOperationException | IllegalArgumentException | FileSystemException e) {
            // OK if the file does not have/does not support the attribute
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesSetPosixFilePermissions() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        try {
            Files.setPosixFilePermissions(file, Set.of());
        } catch (UnsupportedOperationException | IllegalArgumentException | FileSystemException e) {
            // OK if the file does not have/does not support the attribute
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsSymbolicLink() {
        Files.isSymbolicLink(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsDirectory() {
        Files.isDirectory(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsRegularFile() {
        Files.isRegularFile(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesGetLastModifiedTime() throws IOException {
        Files.getLastModifiedTime(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesSetLastModifiedTime() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        Files.setLastModifiedTime(file, FileTime.from(Instant.now()));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesSize() throws IOException {
        Files.size(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesExists() {
        Files.exists(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNotExists() {
        Files.notExists(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsReadable() {
        Files.isReadable(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsWriteable() {
        Files.isWritable(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesIsExecutable() {
        Files.isExecutable(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWalkFileTree() throws IOException {
        Files.walkFileTree(readDir(), dummyVisitor());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWalkFileTreeWithOptions() throws IOException {
        Files.walkFileTree(readDir(), Set.of(FileVisitOption.FOLLOW_LINKS), 2, dummyVisitor());
    }

    private static FileVisitor<Path> dummyVisitor() {
        return new FileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                return FileVisitResult.SKIP_SUBTREE;
            }
        };
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewBufferedReader() throws IOException {
        Files.newBufferedReader(readFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewBufferedReaderWithCharset() throws IOException {
        Files.newBufferedReader(readFile(), Charset.defaultCharset()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewBufferedWriter() throws IOException {
        Files.newBufferedWriter(readWriteFile(), StandardOpenOption.WRITE).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesNewBufferedWriterWithCharset() throws IOException {
        Files.newBufferedWriter(readWriteFile(), Charset.defaultCharset(), StandardOpenOption.WRITE).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCopyInputStream() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.copy(new ByteArrayInputStream("foo".getBytes(StandardCharsets.UTF_8)), directory.resolve("copied"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesCopyOutputStream() throws IOException {
        Files.copy(readFile(), new ByteArrayOutputStream());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadAllBytes() throws IOException {
        Files.readAllBytes(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadString() throws IOException {
        Files.readString(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadStringWithCharset() throws IOException {
        Files.readString(readFile(), Charset.defaultCharset());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadAllLines() throws IOException {
        Files.readAllLines(readFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesReadAllLinesWithCharset() throws IOException {
        Files.readAllLines(readFile(), Charset.defaultCharset());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWrite() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.writeString(directory.resolve("file"), "foo");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWriteLines() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.write(directory.resolve("file"), List.of("foo"));
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWriteString() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.writeString(directory.resolve("file"), "foo");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWriteStringWithCharset() throws IOException {
        var directory = EntitledActions.createTempDirectoryForWrite();
        Files.writeString(directory.resolve("file"), "foo", Charset.defaultCharset());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesList() throws IOException {
        Files.list(readDir()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWalk() throws IOException {
        Files.walk(readDir()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesWalkWithDepth() throws IOException {
        Files.walk(readDir(), 2).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesFind() throws IOException {
        Files.find(readDir(), 2, (path, basicFileAttributes) -> false).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesLines() throws IOException {
        Files.lines(readFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void checkFilesLinesWithCharset() throws IOException {
        Files.lines(readFile(), Charset.defaultCharset()).close();
    }

    private NioFilesActions() {}
}
