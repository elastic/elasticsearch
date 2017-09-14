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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.maxmind.db.NoCache;
import com.maxmind.db.NodeCache;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Randomness;
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

    private static Map<String, DatabaseReaderLazyLoader> databaseReaders;

    @BeforeClass
    public static void loadDatabaseReaders() throws IOException {
        Path configDir = createTempDir();
        Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb.gz")),
                geoIpConfigDir.resolve("GeoLite2-City.mmdb.gz"));
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb.gz")),
                geoIpConfigDir.resolve("GeoLite2-Country.mmdb.gz"));

        NodeCache cache = randomFrom(NoCache.getInstance(), new GeoIpCache(randomNonNegativeLong()));
        databaseReaders = IngestGeoIpPlugin.loadDatabaseReaders(geoIpConfigDir, cache);
    }

    @AfterClass
    public static void closeDatabaseReaders() throws IOException {
        for (DatabaseReaderLazyLoader reader : databaseReaders.values()) {
            reader.close();
        }
        databaseReaders = null;
    }

    public void testBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDbReader().getMetadata().getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDbReader().getMetadata().getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCountryBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb.gz");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDbReader().getMetadata().getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb.gz");
        GeoIpProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDbReader().getMetadata().getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildWithCountryDbAndCityFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb.gz");
        EnumSet<GeoIpProcessor.Property> cityOnlyProperties = EnumSet.complementOf(GeoIpProcessor.Property.ALL_COUNTRY_PROPERTIES);
        String cityProperty = RandomPicks.randomFrom(Randomness.get(), cityOnlyProperties).toString();
        config.put("properties", Collections.singletonList(cityProperty));
        try {
            factory.create(null, null, config);
            fail("Exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[properties] illegal property value [" + cityProperty +
                    "]. valid values are [IP, COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME]"));
        }
    }

    public void testBuildNonExistingDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "does-not-exist.mmdb.gz");
        try {
            factory.create(null, null, config);
            fail("Exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[database_file] database file [does-not-exist.mmdb.gz] doesn't exist"));
        }
    }

    public void testBuildFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Set<GeoIpProcessor.Property> properties = EnumSet.noneOf(GeoIpProcessor.Property.class);
        List<String> fieldNames = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, GeoIpProcessor.Property.values().length);
        for (int i = 0; i < numFields; i++) {
            GeoIpProcessor.Property property = GeoIpProcessor.Property.values()[i];
            properties.add(property);
            fieldNames.add(property.name().toLowerCase(Locale.ROOT));
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        GeoIpProcessor processor = factory.create(null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(properties));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildIllegalFieldOption() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", Collections.singletonList("invalid"));
        try {
            factory.create(null, null, config);
            fail("exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[properties] illegal property value [invalid]. valid values are [IP, COUNTRY_ISO_CODE, " +
                    "COUNTRY_NAME, CONTINENT_NAME, REGION_NAME, CITY_NAME, TIMEZONE, LOCATION]"));
        }

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", "invalid");
        try {
            factory.create(null, null, config);
            fail("exception expected");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
        }
    }

    public void testLazyLoading() throws Exception {
        Path configDir = createTempDir();
        Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-City.mmdb.gz")),
            geoIpConfigDir.resolve("GeoLite2-City.mmdb.gz"));
        Files.copy(new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/GeoLite2-Country.mmdb.gz")),
            geoIpConfigDir.resolve("GeoLite2-Country.mmdb.gz"));

        // Loading another database reader instances, because otherwise we can't test lazy loading as the
        // database readers used at class level are reused between tests. (we want to keep that otherwise running this
        // test will take roughly 4 times more time)
        Map<String, DatabaseReaderLazyLoader> databaseReaders =
            IngestGeoIpPlugin.loadDatabaseReaders(geoIpConfigDir, NoCache.getInstance());
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders);
        for (DatabaseReaderLazyLoader lazyLoader : databaseReaders.values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-City.mmdb.gz");
        factory.create(null, "_tag", config);
        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb.gz");
        factory.create(null, "_tag", config);

        for (DatabaseReaderLazyLoader lazyLoader : databaseReaders.values()) {
            assertNotNull(lazyLoader.databaseReader.get());
        }
    }
}
