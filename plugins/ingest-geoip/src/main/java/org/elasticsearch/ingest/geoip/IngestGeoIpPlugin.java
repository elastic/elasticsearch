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
import com.maxmind.geoip2.DatabaseReader;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

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

        Map<String, DatabaseReaderLazyLoader> databaseReaders = new HashMap<>();
        try (Stream<Path> databaseFiles = Files.list(geoIpConfigDirectory)) {
            PathMatcher pathMatcher = geoIpConfigDirectory.getFileSystem().getPathMatcher("glob:**.mmdb.gz");
            // Use iterator instead of forEach otherwise IOException needs to be caught twice...
            Iterator<Path> iterator = databaseFiles.iterator();
            while (iterator.hasNext()) {
                Path databasePath = iterator.next();
                if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                    String databaseFileName = databasePath.getFileName().toString();
                    DatabaseReaderLazyLoader holder = new DatabaseReaderLazyLoader(databaseFileName, () -> {
                        try (InputStream inputStream = new GZIPInputStream(Files.newInputStream(databasePath, StandardOpenOption.READ))) {
                            return new DatabaseReader.Builder(inputStream).withCache(cache).build();
                        }
                    });
                    databaseReaders.put(databaseFileName, holder);
                }
            }
        }
        return Collections.unmodifiableMap(databaseReaders);
    }

    @Override
    public void close() throws IOException {
        if (databaseReaders != null) {
            IOUtils.close(databaseReaders.values());
        }
    }

}
