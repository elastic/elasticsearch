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

package org.elasticsearch.ingest.processor.geoip;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.junit.Before;

import static org.hamcrest.Matchers.*;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class GeoProcessorFactoryTests extends ESTestCase {

    private Path configDir;

    @Before
    public void prepareConfigDirectory() throws Exception {
        this.configDir = createTempDir();
        Path geoIpConfigDir = configDir.resolve("ingest").resolve("geoip");
        Files.createDirectories(geoIpConfigDir);
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")), geoIpConfigDir.resolve("GeoLite2-City.mmdb"));
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")), geoIpConfigDir.resolve("GeoLite2-Country.mmdb"));
    }

    public void testBuild_defaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory();
        factory.setConfigDirectory(configDir);
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(Collections.emptyMap());
        assertThat(processor.dbReader.getMetadata().getDatabaseType(), equalTo("GeoLite2-City"));
    }

    public void testBuild_dbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory();
        factory.setConfigDirectory(configDir);
        GeoIpProcessor processor = (GeoIpProcessor) factory.create(Collections.singletonMap("database_file", "GeoLite2-Country.mmdb"));
        assertThat(processor.dbReader.getMetadata().getDatabaseType(), equalTo("GeoLite2-Country"));
    }

    public void testBuild_nonExistingDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory();
        factory.setConfigDirectory(configDir);
        try {
            factory.create(Collections.singletonMap("database_file", "does-not-exist.mmdb"));
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), startsWith("database file [does-not-exist.mmdb] doesn't exist in"));
        }
    }


}
