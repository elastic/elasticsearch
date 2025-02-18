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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.util.Scanner;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
class FileCheckActions {

    static Path testRootDir = Paths.get(System.getProperty("es.entitlements.testdir"));

    static Path readDir() {
        return testRootDir.resolve("read_dir");
    }

    static Path readWriteDir() {
        return testRootDir.resolve("read_write_dir");
    }

    static Path readFile() {
        return testRootDir.resolve("read_file");
    }

    static Path readWriteFile() {
        return testRootDir.resolve("read_write_file");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileCanExecute() throws IOException {
        readFile().toFile().canExecute();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileCanRead() throws IOException {
        readFile().toFile().canRead();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileCanWrite() throws IOException {
        readFile().toFile().canWrite();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileCreateNewFile() throws IOException {
        readWriteDir().resolve("new_file").toFile().createNewFile();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileCreateTempFile() throws IOException {
        File.createTempFile("prefix", "suffix", readWriteDir().toFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileDelete() throws IOException {
        Path toDelete = readWriteDir().resolve("to_delete");
        EntitledActions.createFile(toDelete);
        toDelete.toFile().delete();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileDeleteOnExit() throws IOException {
        Path toDelete = readWriteDir().resolve("to_delete_on_exit");
        EntitledActions.createFile(toDelete);
        toDelete.toFile().deleteOnExit();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileExists() throws IOException {
        readFile().toFile().exists();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileIsDirectory() throws IOException {
        readFile().toFile().isDirectory();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileIsFile() throws IOException {
        readFile().toFile().isFile();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileIsHidden() throws IOException {
        readFile().toFile().isHidden();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileLastModified() throws IOException {
        readFile().toFile().lastModified();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileLength() throws IOException {
        readFile().toFile().length();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileList() throws IOException {
        readDir().toFile().list();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileListWithFilter() throws IOException {
        readDir().toFile().list((dir, name) -> true);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileListFiles() throws IOException {
        readDir().toFile().listFiles();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileListFilesWithFileFilter() throws IOException {
        readDir().toFile().listFiles(pathname -> true);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileListFilesWithFilenameFilter() throws IOException {
        readDir().toFile().listFiles((dir, name) -> true);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileMkdir() throws IOException {
        Path mkdir = readWriteDir().resolve("mkdir");
        mkdir.toFile().mkdir();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileMkdirs() throws IOException {
        Path mkdir = readWriteDir().resolve("mkdirs");
        mkdir.toFile().mkdirs();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileRenameTo() throws IOException {
        Path toRename = readWriteDir().resolve("to_rename");
        EntitledActions.createFile(toRename);
        toRename.toFile().renameTo(readWriteDir().resolve("renamed").toFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetExecutable() throws IOException {
        readWriteFile().toFile().setExecutable(false);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetExecutableOwner() throws IOException {
        readWriteFile().toFile().setExecutable(false, false);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetLastModified() throws IOException {
        readWriteFile().toFile().setLastModified(System.currentTimeMillis());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetReadable() throws IOException {
        readWriteFile().toFile().setReadable(true);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetReadableOwner() throws IOException {
        readWriteFile().toFile().setReadable(true, false);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetReadOnly() throws IOException {
        Path readOnly = readWriteDir().resolve("read_only");
        EntitledActions.createFile(readOnly);
        readOnly.toFile().setReadOnly();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetWritable() throws IOException {
        readWriteFile().toFile().setWritable(true);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileSetWritableOwner() throws IOException {
        readWriteFile().toFile().setWritable(true, false);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createScannerFile() throws FileNotFoundException {
        new Scanner(readFile().toFile());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createScannerFileWithCharset() throws IOException {
        new Scanner(readFile().toFile(), StandardCharsets.UTF_8);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createScannerFileWithCharsetName() throws FileNotFoundException {
        new Scanner(readFile().toFile(), "UTF-8");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileOutputStreamString() throws IOException {
        new FileOutputStream(readWriteFile().toString()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileOutputStreamStringWithAppend() throws IOException {
        new FileOutputStream(readWriteFile().toString(), false).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileOutputStreamFile() throws IOException {
        new FileOutputStream(readWriteFile().toFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileOutputStreamFileWithAppend() throws IOException {
        new FileOutputStream(readWriteFile().toFile(), false).close();
    }

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

    private FileCheckActions() {}
}
