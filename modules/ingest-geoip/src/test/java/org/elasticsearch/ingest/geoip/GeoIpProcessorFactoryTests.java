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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin.GeoIpCache;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.sameInstance;

public class GeoIpProcessorFactoryTests extends ESTestCase {

    private static Map<String, DatabaseReaderLazyLoader> databaseReaders;

    @BeforeClass
    public static void loadDatabaseReaders() throws IOException {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);

        databaseReaders = IngestGeoIpPlugin.loadDatabaseReaders(geoIpDir, geoIpConfigDir);
    }

    @AfterClass
    public static void closeDatabaseReaders() throws IOException {
        for (DatabaseReaderLazyLoader reader : databaseReaders.values()) {
            reader.close();
        }
        databaseReaders = null;
    }

    public void testBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-City"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testCountryBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testAsnBuildDefaults() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        String processorTag = randomAlphaOfLength(10);

        GeoIpProcessor processor = factory.create(null, processorTag, null, config);

        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-ASN"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_ASN_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_field");
        GeoIpProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_field"));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        GeoIpProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("geoip"));
        assertThat(processor.getDatabaseType(), equalTo("GeoLite2-Country"));
        assertThat(processor.getProperties(), sameInstance(GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildWithCountryDbAndAsnFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        EnumSet<GeoIpProcessor.Property> asnOnlyProperties = EnumSet.copyOf(GeoIpProcessor.Property.ALL_ASN_PROPERTIES);
        asnOnlyProperties.remove(GeoIpProcessor.Property.IP);
        String asnProperty = RandomPicks.randomFrom(Randomness.get(), asnOnlyProperties).toString();
        config.put("properties", Collections.singletonList(asnProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[properties] illegal property value [" + asnProperty +
            "]. valid values are [IP, COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME]"));
    }

    public void testBuildWithAsnDbAndCityFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        EnumSet<GeoIpProcessor.Property> cityOnlyProperties = EnumSet.copyOf(GeoIpProcessor.Property.ALL_CITY_PROPERTIES);
        cityOnlyProperties.remove(GeoIpProcessor.Property.IP);
        String cityProperty = RandomPicks.randomFrom(Randomness.get(), cityOnlyProperties).toString();
        config.put("properties", Collections.singletonList(cityProperty));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[properties] illegal property value [" + cityProperty +
            "]. valid values are [IP, ASN, ORGANIZATION_NAME]"));
    }

    public void testBuildNonExistingDbFile() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "does-not-exist.mmdb");
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[database_file] database file [does-not-exist.mmdb] doesn't exist"));
    }

    public void testBuildFields() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Set<GeoIpProcessor.Property> properties = EnumSet.noneOf(GeoIpProcessor.Property.class);
        List<String> fieldNames = new ArrayList<>();

        int counter = 0;
        int numFields = scaledRandomIntBetween(1, GeoIpProcessor.Property.values().length);
        for (GeoIpProcessor.Property property : GeoIpProcessor.Property.ALL_CITY_PROPERTIES) {
            properties.add(property);
            fieldNames.add(property.name().toLowerCase(Locale.ROOT));
            if (++counter >= numFields) {
                break;
            }
        }
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);
        GeoIpProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(properties));
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildIllegalFieldOption() throws Exception {
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));

        Map<String, Object> config1 = new HashMap<>();
        config1.put("field", "_field");
        config1.put("properties", Collections.singletonList("invalid"));
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config1));
        assertThat(e.getMessage(), equalTo("[properties] illegal property value [invalid]. valid values are [IP, COUNTRY_ISO_CODE, " +
            "COUNTRY_NAME, CONTINENT_NAME, REGION_ISO_CODE, REGION_NAME, CITY_NAME, TIMEZONE, LOCATION]"));

        Map<String, Object> config2 = new HashMap<>();
        config2.put("field", "_field");
        config2.put("properties", "invalid");
        e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, null, config2));
        assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
    }

    public void testLazyLoading() throws Exception {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);

        // Loading another database reader instances, because otherwise we can't test lazy loading as the
        // database readers used at class level are reused between tests. (we want to keep that otherwise running this
        // test will take roughly 4 times more time)
        Map<String, DatabaseReaderLazyLoader> databaseReaders = IngestGeoIpPlugin.loadDatabaseReaders(geoIpDir, geoIpConfigDir);
        GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));
        for (DatabaseReaderLazyLoader lazyLoader : databaseReaders.values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Collections.singletonMap("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", "routing", 1L, VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-City.mmdb");
        final GeoIpProcessor city = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseReaders.get("GeoLite2-City.mmdb").databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseReaders.get("GeoLite2-City.mmdb").databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-Country.mmdb");
        final GeoIpProcessor country = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseReaders.get("GeoLite2-Country.mmdb").databaseReader.get());
        country.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseReaders.get("GeoLite2-Country.mmdb").databaseReader.get());

        config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoLite2-ASN.mmdb");
        final GeoIpProcessor asn = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseReaders.get("GeoLite2-ASN.mmdb").databaseReader.get());
        asn.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseReaders.get("GeoLite2-ASN.mmdb").databaseReader.get());
    }

    public void testLoadingCustomDatabase() throws IOException {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);
        // fake the GeoIP2-City database
        copyDatabaseFile(geoIpConfigDir, "GeoLite2-City.mmdb");
        Files.move(geoIpConfigDir.resolve("GeoLite2-City.mmdb"), geoIpConfigDir.resolve("GeoIP2-City.mmdb"));

        /*
         * Loading another database reader instances, because otherwise we can't test lazy loading as the database readers used at class
         * level are reused between tests. (we want to keep that otherwise running this test will take roughly 4 times more time).
         */
        final Map<String, DatabaseReaderLazyLoader> databaseReaders = IngestGeoIpPlugin.loadDatabaseReaders(geoIpDir, geoIpConfigDir);
        final GeoIpProcessor.Factory factory = new GeoIpProcessor.Factory(databaseReaders, new GeoIpCache(1000));
        for (DatabaseReaderLazyLoader lazyLoader : databaseReaders.values()) {
            assertNull(lazyLoader.databaseReader.get());
        }

        final Map<String, Object> field = Collections.singletonMap("_field", "1.1.1.1");
        final IngestDocument document = new IngestDocument("index", "id", "routing", 1L, VersionType.EXTERNAL, field);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("database_file", "GeoIP2-City.mmdb");
        final GeoIpProcessor city = factory.create(null, "_tag", null, config);

        // these are lazy loaded until first use so we expect null here
        assertNull(databaseReaders.get("GeoIP2-City.mmdb").databaseReader.get());
        city.execute(document);
        // the first ingest should trigger a database load
        assertNotNull(databaseReaders.get("GeoIP2-City.mmdb").databaseReader.get());
    }

    public void testDatabaseNotExistsInDir() throws IOException {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        if (randomBoolean()) {
            Files.createDirectories(geoIpConfigDir);
        }
        copyDatabaseFiles(geoIpDir);
        final String databaseFilename = randomFrom(IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES);
        Files.delete(geoIpDir.resolve(databaseFilename));
        final IOException e =
                expectThrows(IOException.class, () -> IngestGeoIpPlugin.loadDatabaseReaders(geoIpDir, geoIpConfigDir));
        assertThat(e, hasToString(containsString("expected database [" + databaseFilename + "] to exist in [" + geoIpDir + "]")));
    }

    public void testDatabaseExistsInConfigDir() throws IOException {
        final Path geoIpDir = createTempDir();
        final Path configDir = createTempDir();
        final Path geoIpConfigDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpConfigDir);
        copyDatabaseFiles(geoIpDir);
        final String databaseFilename = randomFrom(IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES);
        copyDatabaseFile(geoIpConfigDir, databaseFilename);
        final IOException e =
                expectThrows(IOException.class, () -> IngestGeoIpPlugin.loadDatabaseReaders(geoIpDir, geoIpConfigDir));
        assertThat(e, hasToString(containsString("expected database [" + databaseFilename + "] to not exist in [" + geoIpConfigDir + "]")));
    }

    private static void copyDatabaseFile(final Path path, final String databaseFilename) throws IOException {
        Files.copy(
                new ByteArrayInputStream(StreamsUtils.copyToBytesFromClasspath("/" + databaseFilename)),
                path.resolve(databaseFilename));
    }

    private static void copyDatabaseFiles(final Path path) throws IOException {
        for (final String databaseFilename : IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES) {
            copyDatabaseFile(path, databaseFilename);
        }
    }

}
