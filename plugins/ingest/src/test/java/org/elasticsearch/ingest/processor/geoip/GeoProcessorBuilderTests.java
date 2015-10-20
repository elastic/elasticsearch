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

public class GeoProcessorBuilderTests extends ESTestCase {

    private Path geoIpConfigDir;

    @Before
    public void prepareConfigDirectory() throws Exception {
        geoIpConfigDir = createTempDir();
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")), geoIpConfigDir.resolve("GeoLite2-City.mmdb"));
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")), geoIpConfigDir.resolve("GeoLite2-Country.mmdb"));
    }

    public void testBuild_defaults() throws Exception {
        GeoIpProcessor.Builder builder = new GeoIpProcessor.Builder(geoIpConfigDir, new DatabaseReaderService());
        builder.fromMap(Collections.emptyMap());
        GeoIpProcessor processor = (GeoIpProcessor) builder.build();
        assertThat(processor.dbReader.getMetadata().getDatabaseType(), equalTo("GeoLite2-City"));
    }

    public void testBuild_dbFile() throws Exception {
        GeoIpProcessor.Builder builder = new GeoIpProcessor.Builder(geoIpConfigDir, new DatabaseReaderService());
        builder.fromMap(Collections.singletonMap("database_file", "GeoLite2-Country.mmdb"));
        GeoIpProcessor processor = (GeoIpProcessor) builder.build();
        assertThat(processor.dbReader.getMetadata().getDatabaseType(), equalTo("GeoLite2-Country"));
    }

    public void testBuild_nonExistingDbFile() throws Exception {
        GeoIpProcessor.Builder builder = new GeoIpProcessor.Builder(geoIpConfigDir, new DatabaseReaderService());
        builder.fromMap(Collections.singletonMap("database_file", "does-not-exist.mmdb"));
        try {
            builder.build();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), startsWith("database file [does-not-exist.mmdb] doesn't exist in"));
        }
    }


}
