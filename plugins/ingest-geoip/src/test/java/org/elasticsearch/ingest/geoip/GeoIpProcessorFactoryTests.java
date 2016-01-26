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
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.ingest.processor.ConfigurationPropertyException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class GeoIpProcessorFactoryTests extends ESTestCase {

    private static Map<String, DatabaseReader> databaseReaders;

    @BeforeClass
    public static void loadDatabaseReaders() throws IOException {
        Path configDir = createTempDir();
        Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb")), geoIpConfigDir.resolve("GeoLite2-City.mmdb"));
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb")), geoIpConfigDir.resolve("GeoLite2-Country.mmdb"));
        databaseReaders = IngestGeoIpPlugin.loadDatabaseReaders(geoIpConfigDir);
    }

    @AfterClass
    public static void closeDatabaseReaders() throws IOException {
        for (DatabaseReader reader : databaseReaders.values()) {
            reader.close();
        }
        databaseReaders = null;
    }

    public void testBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "_field");

        String processorTag = randomAsciiOfLength(10);
        config.put(AbstractProcessorFactory.TAG_KEY, processorTag);

        GeoIpProcessor processor = factory.create(config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getSourceField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDbReader().getMetadata().getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getFields(), sameInstance(GeoIpProcessor.Factory.DEFAULT_FIELDS));
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);
        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = factory.create(config);
        assertThat(processor.getSourceField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);
        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        GeoIpProcessor processor = factory.create(config);
        assertThat(processor.getSourceField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDbReader().getMetadata().getDatabaseType(), equalTo("GeoLite2-Country"));
    }

    public void testBuildNonExistingDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "_field");
        config.put("database_file", "does-not-exist.mmdb");
        try {
            factory.create(config);
            fail("Exception expected");
        } catch (ConfigurationPropertyException e) {
            assertThat(e.getMessage(), equalTo("[database_file] database file [does-not-exist.mmdb] doesn't exist"));
        }
    }

    public void testBuildFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Set<GeoIpProcessor.Field> fields = EnumSet.noneOf(GeoIpProcessor.Field.class);
        List<String> fieldNames = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, GeoIpProcessor.Field.values().length);
        for (int i = 0; i < numFields; i++) {
            GeoIpProcessor.Field field = GeoIpProcessor.Field.values()[i];
            fields.add(field);
            fieldNames.add(field.name().toLowerCase(Locale.ROOT));
        }
        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "_field");
        config.put("fields", fieldNames);
        GeoIpProcessor processor = factory.create(config);
        assertThat(processor.getSourceField(), equalTo("_field"));
        assertThat(processor.getFields(), equalTo(fields));
    }

    public void testBuildIllegalFieldOption() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("source_field", "_field");
        config.put("fields", Collections.singletonList("invalid"));
        try {
            factory.create(config);
            fail("exception expected");
        } catch (ConfigurationPropertyException e) {
            assertThat(e.getMessage(), equalTo("[fields] illegal field option [invalid]. valid values are [[IP, COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME, REGION_NAME, CITY_NAME, TIMEZONE, LATITUDE, LONGITUDE, LOCATION]]"));
        }

        config = new HashMap<>();
        config.put("source_field", "_field");
        config.put("fields", "invalid");
        try {
            factory.create(config);
            fail("exception expected");
        } catch (ConfigurationPropertyException e) {
            assertThat(e.getMessage(), equalTo("[fields] property isn't a list, but of type [java.lang.String]"));
        }
    }
}
