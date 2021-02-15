/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class IngestGeoIpPlugin extends Plugin implements IngestPlugin, Closeable {
    public static final Setting<Long> CACHE_SIZE =
        Setting.longSetting("ingest.geoip.cache_size", 1000, 0, Setting.Property.NodeScope);

    static String[] DEFAULT_DATABASE_FILENAMES = new String[]{"GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"};

    private Map<String, DatabaseReaderLazyLoader> databaseReaders;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(CACHE_SIZE);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        if (databaseReaders != null) {
            throw new IllegalStateException("getProcessors called twice for geoip plugin!!");
        }
        final long cacheSize = CACHE_SIZE.get(parameters.env.settings());
        final GeoIpCache cache = new GeoIpCache(cacheSize);
        final Path geoIpDirectory = getGeoIpDirectory(parameters);
        final Path geoIpConfigDirectory = parameters.env.configFile().resolve("ingest-geoip");
        try {
            databaseReaders = loadDatabaseReaders(cache, geoIpDirectory, geoIpConfigDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Collections.singletonMap(GeoIpProcessor.TYPE, new GeoIpProcessor.Factory(databaseReaders));
    }

    /*
     * In GeoIpProcessorNonIngestNodeTests, ingest-geoip is loaded on the classpath. This means that the plugin is never unbundled into a
     * directory where the database files would live. Therefore, we have to copy these database files ourselves. To do this, we need the
     * ability to specify where those database files would go. We do this by adding a plugin that registers ingest.geoip.database_path as
     * an actual setting. Otherwise, in production code, this setting is not registered and the database path is not configurable.
     */
    @SuppressForbidden(reason = "PathUtils#get")
    private Path getGeoIpDirectory(Processor.Parameters parameters) {
        final Path geoIpDirectory;
        if (parameters.env.settings().get("ingest.geoip.database_path") == null) {
            geoIpDirectory = parameters.env.modulesFile().resolve("ingest-geoip");
        } else {
            geoIpDirectory = PathUtils.get(parameters.env.settings().get("ingest.geoip.database_path"));
        }
        return geoIpDirectory;
    }

    static Map<String, DatabaseReaderLazyLoader> loadDatabaseReaders(GeoIpCache cache,
                                                                     Path geoIpDirectory,
                                                                     Path geoIpConfigDirectory) throws IOException {
        assertDatabaseExistence(geoIpDirectory, true);
        assertDatabaseExistence(geoIpConfigDirectory, false);
        final boolean loadDatabaseOnHeap = Booleans.parseBoolean(System.getProperty("es.geoip.load_db_on_heap", "false"));
        final Map<String, DatabaseReaderLazyLoader> databaseReaders = new HashMap<>();

        // load the default databases
        for (final String databaseFilename : DEFAULT_DATABASE_FILENAMES) {
            final Path databasePath = geoIpDirectory.resolve(databaseFilename);
            final DatabaseReaderLazyLoader loader = createLoader(cache, databasePath, loadDatabaseOnHeap);
            databaseReaders.put(databaseFilename, loader);
        }

        // load any custom databases
        if (Files.exists(geoIpConfigDirectory)) {
            try (Stream<Path> databaseFiles = Files.list(geoIpConfigDirectory)) {
                PathMatcher pathMatcher = geoIpConfigDirectory.getFileSystem().getPathMatcher("glob:**.mmdb");
                // Use iterator instead of forEach otherwise IOException needs to be caught twice...
                Iterator<Path> iterator = databaseFiles.iterator();
                while (iterator.hasNext()) {
                    Path databasePath = iterator.next();
                    if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                        String databaseFileName = databasePath.getFileName().toString();
                        final DatabaseReaderLazyLoader loader = createLoader(cache, databasePath, loadDatabaseOnHeap);
                        databaseReaders.put(databaseFileName, loader);
                    }
                }
            }
        }
        return Collections.unmodifiableMap(databaseReaders);
    }

    private static DatabaseReaderLazyLoader createLoader(GeoIpCache cache, Path databasePath, boolean loadDatabaseOnHeap) {
        CheckedSupplier<DatabaseReader, IOException> loader = () -> {
            DatabaseReader.Builder builder = createDatabaseBuilder(databasePath).withCache(NoCache.getInstance());
            if (loadDatabaseOnHeap) {
                builder.fileMode(Reader.FileMode.MEMORY);
            } else {
                builder.fileMode(Reader.FileMode.MEMORY_MAPPED);
            }
            return builder.build();
        };
        return new DatabaseReaderLazyLoader(cache, databasePath, loader);
    }

    private static void assertDatabaseExistence(final Path path, final boolean exists) throws IOException {
        for (final String database : DEFAULT_DATABASE_FILENAMES) {
            if (Files.exists(path.resolve(database)) != exists) {
                final String message = "expected database [" + database + "] to " + (exists ? "" : "not ") + "exist in [" + path + "]";
                throw new IOException(message);
            }
        }
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private static DatabaseReader.Builder createDatabaseBuilder(Path databasePath) {
        return new DatabaseReader.Builder(databasePath.toFile());
    }

    @Override
    public void close() throws IOException {
        if (databaseReaders != null) {
            IOUtils.close(databaseReaders.values());
        }
    }

}
