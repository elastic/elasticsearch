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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;
import static org.elasticsearch.entitlement.qa.test.FileCheckActions.readFile;

@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
@SuppressWarnings("unused") // Called via reflection
public class JavaBaseFileActions {
    @EntitlementTest(expectedAccess = PLUGINS)
    static void keystore_getInstance_1() throws IOException {
        try {
            KeyStore.getInstance(readFile().toFile(), new char[0]);
        } catch (GeneralSecurityException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void keystore_getInstance_2() throws IOException {
        try {
            KeyStore.LoadStoreParameter loadStoreParameter = () -> null;
            KeyStore.getInstance(readFile().toFile(), loadStoreParameter);
        } catch (GeneralSecurityException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void keystoreBuilder_newInstance() {
        try {
            KeyStore.Builder.newInstance("", null, readFile().toFile(), null);
        } catch (NullPointerException expected) {
            return;
        }
        throw new AssertionError("Expected an exception");
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_1() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toString()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_2() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toString(), defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_3() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_4() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile(), defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_5_readOnly() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile(), OPEN_READ).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_5_readAndDelete() throws IOException {
        expectZipException(() -> new ZipFile(createTempFileForWrite().toFile(), OPEN_READ | OPEN_DELETE).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_6_readOnly() throws IOException {
        expectZipException(() -> new ZipFile(readFile().toFile(), OPEN_READ, defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void zipFile_6_readAndDelete() throws IOException {
        expectZipException(() -> new ZipFile(createTempFileForWrite().toFile(), OPEN_READ | OPEN_DELETE, defaultCharset()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_1() throws IOException {
        expectZipException(() -> new JarFile(readFile().toString()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_2() throws IOException {
        expectZipException(() -> new JarFile(readFile().toString(), false).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_3_readOnly() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile(), false, OPEN_READ).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_3_readAndDelete() throws IOException {
        expectZipException(() -> new JarFile(createTempFileForWrite().toFile(), false, OPEN_READ | OPEN_DELETE).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_4_readOnly() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile(), false, OPEN_READ, Runtime.version()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_4_readAndDelete() throws IOException {
        expectZipException(() -> new JarFile(createTempFileForWrite().toFile(), false, OPEN_READ | OPEN_DELETE, Runtime.version()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_5() throws IOException {
        expectZipException(() -> new JarFile(readFile().toFile()).close());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void jarFile_6() throws IOException {
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

}
