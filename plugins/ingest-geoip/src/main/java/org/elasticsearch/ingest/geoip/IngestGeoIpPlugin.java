/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NoCache;
import com.maxmind.db.NodeCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class IngestGeoIpPlugin extends Plugin implements IngestPlugin, Closeable {
    public static final Setting<Long> CACHE_SIZE =
        Setting.longSetting("ingest.geoip.cache_size", 1000, 0, Setting.Property.NodeScope);

    private Map<String, DatabaseReaderLazyLoader> databaseReaders;

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CACHE_SIZE);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        if (databaseReaders != null) {
            throw new IllegalStateException("getProcessors called twice for geoip plugin!!");
        }
        Path geoIpConfigDirectory = parameters.env.configFile().resolve("ingest-geoip");
        NodeCache cache;
        long cacheSize = CACHE_SIZE.get(parameters.env.settings());
        if (cacheSize > 0) {
            cache = new GeoIpCache(cacheSize);
        } else {
            cache = NoCache.getInstance();
        }
        try {
            databaseReaders = loadDatabaseReaders(geoIpConfigDirectory, cache);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return Collections.singletonMap(GeoIpProcessor.TYPE, new GeoIpProcessor.Factory(databaseReaders));
    }

    static Map<String, DatabaseReaderLazyLoader> loadDatabaseReaders(Path geoIpConfigDirectory, NodeCache cache) throws IOException {
        if (Files.exists(geoIpConfigDirectory) == false && Files.isDirectory(geoIpConfigDirectory)) {
            throw new IllegalStateException("the geoip directory [" + geoIpConfigDirectory  + "] containing databases doesn't exist");
        }
        boolean loadDatabaseOnHeap = Booleans.parseBoolean(System.getProperty("es.geoip.load_db_on_heap", "false"));
        Map<String, DatabaseReaderLazyLoader> databaseReaders = new HashMap<>();
        try (Stream<Path> databaseFiles = Files.list(geoIpConfigDirectory)) {
            PathMatcher pathMatcher = geoIpConfigDirectory.getFileSystem().getPathMatcher("glob:**.mmdb");
            // Use iterator instead of forEach otherwise IOException needs to be caught twice...
            Iterator<Path> iterator = databaseFiles.iterator();
            while (iterator.hasNext()) {
                Path databasePath = iterator.next();
                if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                    String databaseFileName = databasePath.getFileName().toString();
                    DatabaseReaderLazyLoader holder = new DatabaseReaderLazyLoader(databaseFileName,
                        () -> {
                            DatabaseReader.Builder builder = createDatabaseBuilder(databasePath).withCache(cache);
                            if (loadDatabaseOnHeap) {
                                builder.fileMode(Reader.FileMode.MEMORY);
                            } else {
                                builder.fileMode(Reader.FileMode.MEMORY_MAPPED);
                            }
                            return builder.build();
                        });
                    databaseReaders.put(databaseFileName, holder);
                }
            }
        }
        return Collections.unmodifiableMap(databaseReaders);
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
