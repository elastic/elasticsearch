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

import com.maxmind.geoip2.DatabaseReader;
import org.elasticsearch.node.NodeModule;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class IngestGeoIpPlugin extends Plugin {

    public void onModule(NodeModule nodeModule) throws IOException {
        Path geoIpConfigDirectory = nodeModule.getNode().getEnvironment().configFile().resolve("ingest-geoip");
        Map<String, DatabaseReader> databaseReaders = loadDatabaseReaders(geoIpConfigDirectory);
        nodeModule.registerProcessor(GeoIpProcessor.TYPE, (registry) -> new GeoIpProcessor.Factory(databaseReaders));
    }

    public static Map<String, DatabaseReader> loadDatabaseReaders(Path geoIpConfigDirectory) throws IOException {
        if (Files.exists(geoIpConfigDirectory) == false && Files.isDirectory(geoIpConfigDirectory)) {
            throw new IllegalStateException("the geoip directory [" + geoIpConfigDirectory  + "] containing databases doesn't exist");
        }

        Map<String, DatabaseReader> databaseReaders = new HashMap<>();
        try (Stream<Path> databaseFiles = Files.list(geoIpConfigDirectory)) {
            PathMatcher pathMatcher = geoIpConfigDirectory.getFileSystem().getPathMatcher("glob:**.mmdb.gz");
            // Use iterator instead of forEach otherwise IOException needs to be caught twice...
            Iterator<Path> iterator = databaseFiles.iterator();
            while (iterator.hasNext()) {
                Path databasePath = iterator.next();
                if (Files.isRegularFile(databasePath) && pathMatcher.matches(databasePath)) {
                    try (InputStream inputStream = new GZIPInputStream(Files.newInputStream(databasePath, StandardOpenOption.READ))) {
                        databaseReaders.put(databasePath.getFileName().toString(), new DatabaseReader.Builder(inputStream).build());
                    }
                }
            }
        }
        return Collections.unmodifiableMap(databaseReaders);
    }
}
