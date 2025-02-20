/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Scanner;
import java.util.jar.JarFile;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import static java.nio.charset.Charset.defaultCharset;
import static java.util.zip.ZipFile.OPEN_DELETE;
import static java.util.zip.ZipFile.OPEN_READ;
import static org.elasticsearch.entitlement.qa.entitled.EntitledActions.createTempFileForWrite;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
@SuppressWarnings("unused") // Called via reflection
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
    static void createFileInputStreamFile() throws IOException {
        new FileInputStream(readFile().toFile()).close();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createFileInputStreamFileDescriptor() throws IOException {
        new FileInputStream(FileDescriptor.in).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileInputStreamString() throws IOException {
        new FileInputStream(readFile().toString()).close();
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

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createFileOutputStreamFileDescriptor() throws IOException {
        new FileOutputStream(FileDescriptor.out).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileReaderFile() throws IOException {
        new FileReader(readFile().toFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileReaderFileCharset() throws IOException {
        new FileReader(readFile().toFile(), StandardCharsets.UTF_8).close();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createFileReaderFileDescriptor() throws IOException {
        new FileReader(FileDescriptor.in).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileReaderString() throws IOException {
        new FileReader(readFile().toString()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileReaderStringCharset() throws IOException {
        new FileReader(readFile().toString(), StandardCharsets.UTF_8).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterFile() throws IOException {
        new FileWriter(readWriteFile().toFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterFileWithAppend() throws IOException {
        new FileWriter(readWriteFile().toFile(), false).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterFileCharsetWithAppend() throws IOException {
        new FileWriter(readWriteFile().toFile(), StandardCharsets.UTF_8, false).close();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createFileWriterFileDescriptor() throws IOException {
        new FileWriter(FileDescriptor.out).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterString() throws IOException {
        new FileWriter(readWriteFile().toString()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterStringWithAppend() throws IOException {
        new FileWriter(readWriteFile().toString(), false).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterStringCharset() throws IOException {
        new FileWriter(readWriteFile().toString(), StandardCharsets.UTF_8).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createFileWriterStringCharsetWithAppend() throws IOException {
        new FileWriter(readWriteFile().toString(), StandardCharsets.UTF_8, false).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createRandomAccessFileStringRead() throws IOException {
        new RandomAccessFile(readFile().toString(), "r").close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createRandomAccessFileStringReadWrite() throws IOException {
        new RandomAccessFile(readWriteFile().toString(), "rw").close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createRandomAccessFileRead() throws IOException {
        new RandomAccessFile(readFile().toFile(), "r").close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createRandomAccessFileReadWrite() throws IOException {
        new RandomAccessFile(readWriteFile().toFile(), "rw").close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void keystoreGetInstance_FileCharArray() throws IOException {
        try {
            KeyStore.getInstance(readFile().toFile(), new char[0]);
        } catch (GeneralSecurityException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void keystoreGetInstance_FileLoadStoreParameter() throws IOException {
        try {
            KeyStore.LoadStoreParameter loadStoreParameter = () -> null;
            KeyStore.getInstance(readFile().toFile(), loadStoreParameter);
        } catch (GeneralSecurityException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void keystoreBuilderNewInstance() {
        try {
            KeyStore.Builder.newInstance("", null, readFile().toFile(), null);
        } catch (NullPointerException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_String() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toString()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_StringCharset() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toString(), defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_File() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_FileCharset() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile(), defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_FileReadOnly() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile(), OPEN_READ).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_FileReadAndDelete() throws IOException {
        expectZipException(() -> new ZipFile(createTempFileForWrite().toFile(), OPEN_READ | OPEN_DELETE).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_ReadOnlyCharset() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile(), OPEN_READ, defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_ReadAndDeleteCharset() throws IOException {
        expectZipException(() -> new ZipFile(createTempFileForWrite().toFile(), OPEN_READ | OPEN_DELETE, defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_String() throws IOException {
        expectZipException(() -> new JarFile(readFile().toString()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_StringBoolean() throws IOException {
        expectZipException(() -> new JarFile(readFile().toString(), false).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_FileReadOnly() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile(), false, OPEN_READ).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_FileReadAndDelete() throws IOException {
        expectZipException(() -> new JarFile(createTempFileForWrite().toFile(), false, OPEN_READ | OPEN_DELETE).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_FileBooleanReadOnlyVersion() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile(), false, OPEN_READ, Runtime.version()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_FileBooleanReadAndDeleteOnlyVersion() throws IOException {
        expectZipException(() -> new JarFile(createTempFileForWrite().toFile(), false, OPEN_READ | OPEN_DELETE, Runtime.version()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_File() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFileFileBoolean() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile(), false).close());
    }

    private static void expectZipException(CheckedRunnable<IOException> action) throws IOException {
        try {
            action.run();
        } catch (ZipException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
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

    private FileCheckActions() {}
}
