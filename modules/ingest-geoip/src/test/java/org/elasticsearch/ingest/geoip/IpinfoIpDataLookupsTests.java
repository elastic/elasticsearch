/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IpinfoIpDataLookupsTests extends ESTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(ConfigDatabases.class.getSimpleName());
        Settings settings = Settings.builder().put("resource.reload.interval.high", TimeValue.timeValueMillis(100)).build();
        resourceWatcherService = new ResourceWatcherService(settings, threadPool);
    }

    @After
    public void cleanup() {
        resourceWatcherService.close();
        threadPool.shutdownNow();
    }

    public void testDatabasePropertyInvariants() {
        // the second ASN variant database is like a specialization of the ASN database
        assertThat(Sets.difference(Database.Asn.properties(), Database.AsnV2.properties()), is(empty()));
        assertThat(Database.Asn.defaultProperties(), equalTo(Database.AsnV2.defaultProperties()));
    }

    public void testCountry() throws IOException {
        Path configDir = createTempDir();
        copyDatabase("ipinfo/ip_country_sample.mmdb", configDir.resolve("ip_country_sample.mmdb"));

        GeoIpCache cache = new GeoIpCache(1000); // real cache to test purging of entries upon a reload
        ConfigDatabases configDatabases = new ConfigDatabases(configDir, cache);
        configDatabases.initialize(resourceWatcherService);

        // this is the 'free' Country database (sample)
        {
            DatabaseReaderLazyLoader loader = configDatabases.getDatabase("ip_country_sample.mmdb");
            IpDataLookup lookup = new IpinfoIpDataLookups.Country(Set.of(Database.Property.values()));
            Map<String, Object> data = lookup.getData(loader, "4.221.143.168");
            assertThat(
                data,
                equalTo(
                    Map.ofEntries(
                        entry("ip", "4.221.143.168"),
                        entry("country_name", "South Africa"),
                        entry("country_iso_code", "ZA"),
                        entry("continent_name", "Africa"),
                        entry("continent_code", "AF")
                    )
                )
            );
        }
    }
}
