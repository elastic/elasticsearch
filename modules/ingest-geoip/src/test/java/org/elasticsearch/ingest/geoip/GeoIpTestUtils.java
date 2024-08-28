/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.SuppressForbidden;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class GeoIpTestUtils {

    private GeoIpTestUtils() {
        // utility class
    }

    public static final Set<String> DEFAULT_DATABASES = Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb");

    @SuppressForbidden(reason = "uses java.io.File")
    private static boolean isDirectory(final Path path) {
        return path.toFile().isDirectory();
    }

    public static void copyDatabase(final String databaseName, final Path destination) {
        try (InputStream is = GeoIpTestUtils.class.getResourceAsStream("/" + databaseName)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + databaseName + "] not found in classpath");
            }

            Files.copy(is, isDirectory(destination) ? destination.resolve(databaseName) : destination, REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void copyDefaultDatabases(final Path directory) {
        for (final String database : DEFAULT_DATABASES) {
            copyDatabase(database, directory);
        }
    }

    public static void copyDefaultDatabases(final Path directory, ConfigDatabases configDatabases) {
        for (final String database : DEFAULT_DATABASES) {
            copyDatabase(database, directory);
            configDatabases.updateDatabase(directory.resolve(database), true);
        }
    }
}
