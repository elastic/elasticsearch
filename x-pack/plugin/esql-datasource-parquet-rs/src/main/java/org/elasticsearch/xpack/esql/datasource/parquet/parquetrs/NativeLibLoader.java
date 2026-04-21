/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * Loads the native parquet-rs shared library from the classpath.
 * <p>
 * Loading failures are captured rather than thrown so that a missing native library
 * produces a clean query error instead of crashing the node via an uncaught
 * {@link UnsatisfiedLinkError} or {@link ExceptionInInitializerError}.
 */
final class NativeLibLoader {

    private static final Logger logger = LogManager.getLogger(NativeLibLoader.class);

    private static volatile boolean loaded = false;
    private static volatile String loadError = null;

    private NativeLibLoader() {}

    /**
     * Ensures the native library has been loaded successfully.
     *
     * @throws IllegalStateException if the library could not be loaded
     */
    static void ensureLoaded() {
        if (loaded == false && loadError == null) {
            load();
        }
        if (loadError != null) {
            throw new IllegalStateException("Native parquet-rs library is not available: " + loadError);
        }
    }

    private static synchronized void load() {
        if (loaded || loadError != null) {
            return;
        }
        try {
            doLoad();
        } catch (Throwable t) {
            loadError = t.getMessage();
            logger.warn("Failed to load native parquet-rs library: [{}]", loadError);
        }
    }

    private static void doLoad() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

        String libName;
        if (os.contains("mac") || os.contains("darwin")) {
            libName = "libesql_parquet_rs.dylib";
        } else if (os.contains("linux")) {
            libName = "libesql_parquet_rs.so";
        } else {
            throw new UnsupportedOperationException("Unsupported OS: " + os);
        }

        String platform;
        if (arch.contains("aarch64") || arch.contains("arm64")) {
            platform = os.contains("mac") ? "darwin-aarch64" : "linux-aarch64";
        } else if (arch.contains("amd64") || arch.contains("x86_64")) {
            platform = os.contains("mac") ? "darwin-x86_64" : "linux-x86_64";
        } else {
            throw new UnsupportedOperationException("Unsupported architecture: " + arch);
        }

        String resourcePath = "/platform/" + platform + "/" + libName;
        try (InputStream is = NativeLibLoader.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new UnsatisfiedLinkError("Native library not found on classpath: " + resourcePath);
            }
            Path tmpDir = createTempDirectory();
            Path tmpLib = tmpDir.resolve(libName);
            Files.copy(is, tmpLib, StandardCopyOption.REPLACE_EXISTING);
            registerDeleteOnExit(tmpLib, tmpDir);

            System.load(tmpLib.toAbsolutePath().toString());
            loaded = true;
            logger.info("Loaded native parquet-rs library from [{}]", resourcePath);
        } catch (IOException e) {
            throw new UnsatisfiedLinkError("Failed to extract native library: " + e.getMessage());
        }
    }

    @SuppressForbidden(reason = "Native library extraction requires a temp directory")
    private static Path createTempDirectory() throws IOException {
        return Files.createTempDirectory("esql-native-");
    }

    @SuppressForbidden(reason = "Cleanup of extracted native library on JVM shutdown")
    private static void registerDeleteOnExit(Path tmpLib, Path tmpDir) {
        tmpLib.toFile().deleteOnExit();
        tmpDir.toFile().deleteOnExit();
    }
}
