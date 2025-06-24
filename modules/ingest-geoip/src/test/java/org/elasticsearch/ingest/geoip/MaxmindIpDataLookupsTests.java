/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.util.Constants;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MaxmindIpDataLookupsTests extends ESTestCase {

    // a temporary directory that mmdb files can be copied to and read from
    private Path tmpDir;

    @Before
    public void setup() {
        tmpDir = createTempDir();
    }

    @After
    public void cleanup() throws IOException {
        IOUtils.rm(tmpDir);
    }

    public void testCity() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-City.mmdb";
        String ip = "8.8.8.8";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.City(Database.City.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("country_in_european_union", false),
                entry("country_iso_code", "US"),
                entry("country_name", "United States"),
                entry("continent_code", "NA"),
                entry("continent_name", "North America"),
                entry("timezone", "America/Chicago"),
                entry("location", Map.of("lat", 37.751d, "lon", -97.822d)),
                entry("accuracy_radius", 1000),
                entry("registered_country_in_european_union", false),
                entry("registered_country_iso_code", "US"),
                entry("registered_country_name", "United States")
            )
        );
    }

    public void testCity_withIpV6() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-City.mmdb";
        String ip = "2602:306:33d3:8000::3257:9652";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.City(Database.City.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("country_in_european_union", false),
                entry("country_iso_code", "US"),
                entry("country_name", "United States"),
                entry("continent_code", "NA"),
                entry("continent_name", "North America"),
                entry("region_iso_code", "US-FL"),
                entry("region_name", "Florida"),
                entry("city_name", "Homestead"),
                entry("postal_code", "33035"),
                entry("timezone", "America/New_York"),
                entry("location", Map.of("lat", 25.4573d, "lon", -80.4572d)),
                entry("accuracy_radius", 50),
                entry("registered_country_in_european_union", false),
                entry("registered_country_iso_code", "US"),
                entry("registered_country_name", "United States")
            )
        );
    }

    public void testCityWithMissingLocation() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-City.mmdb";
        String ip = "80.231.5.0";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.City(Database.City.properties()),
            Map.ofEntries(entry("ip", ip))
        );
    }

    public void testCountry() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-Country.mmdb";
        String ip = "82.170.213.79";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.Country(Database.Country.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("country_in_european_union", true),
                entry("country_iso_code", "NL"),
                entry("country_name", "Netherlands"),
                entry("continent_code", "EU"),
                entry("continent_name", "Europe"),
                entry("registered_country_in_european_union", true),
                entry("registered_country_iso_code", "NL"),
                entry("registered_country_name", "Netherlands")
            )
        );
    }

    /**
     * Don't silently do DNS lookups or anything trappy on bogus data
     */
    public void testInvalid() throws IOException {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-Country.mmdb";
        String ip = "www.google.com";
        try (DatabaseReaderLazyLoader loader = loader(databaseName)) {
            IpDataLookup lookup = new MaxmindIpDataLookups.Country(Database.Country.properties());
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> lookup.getData(loader, ip));
            assertThat(e.getMessage(), containsString("not an IP string literal"));
        }
    }

    public void testCountryWithMissingLocation() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-Country.mmdb";
        String ip = "80.231.5.0";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.Country(Database.Country.properties()),
            Map.ofEntries(entry("ip", ip))
        );
    }

    public void testAsn() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoLite2-ASN.mmdb";
        String ip = "82.171.64.0";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.Asn(Database.Asn.properties()),
            Map.ofEntries(entry("ip", ip), entry("organization_name", "KPN B.V."), entry("asn", 1136L), entry("network", "82.168.0.0/14"))
        );
    }

    public void testAnonymousIp() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoIP2-Anonymous-IP-Test.mmdb";
        String ip = "81.2.69.1";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.AnonymousIp(Database.AnonymousIp.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("hosting_provider", true),
                entry("tor_exit_node", true),
                entry("anonymous_vpn", true),
                entry("anonymous", true),
                entry("public_proxy", true),
                entry("residential_proxy", true)
            )
        );
    }

    public void testConnectionType() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoIP2-Connection-Type-Test.mmdb";
        String ip = "214.78.120.5";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.ConnectionType(Database.ConnectionType.properties()),
            Map.ofEntries(entry("ip", ip), entry("connection_type", "Satellite"))
        );
    }

    public void testDomain() {
        String databaseName = "GeoIP2-Domain-Test.mmdb";
        String ip = "69.219.64.2";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.Domain(Database.Domain.properties()),
            Map.ofEntries(entry("ip", ip), entry("domain", "ameritech.net"))
        );
    }

    public void testEnterprise() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoIP2-Enterprise-Test.mmdb";
        String ip = "74.209.24.4";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.Enterprise(Database.Enterprise.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("country_confidence", 99),
                entry("country_in_european_union", false),
                entry("country_iso_code", "US"),
                entry("country_name", "United States"),
                entry("continent_code", "NA"),
                entry("continent_name", "North America"),
                entry("region_iso_code", "US-NY"),
                entry("region_name", "New York"),
                entry("city_confidence", 11),
                entry("city_name", "Chatham"),
                entry("timezone", "America/New_York"),
                entry("location", Map.of("lat", 42.3478, "lon", -73.5549)),
                entry("accuracy_radius", 27),
                entry("postal_code", "12037"),
                entry("postal_confidence", 11),
                entry("asn", 14671L),
                entry("organization_name", "FairPoint Communications"),
                entry("network", "74.209.16.0/20"),
                entry("hosting_provider", false),
                entry("tor_exit_node", false),
                entry("anonymous_vpn", false),
                entry("anonymous", false),
                entry("public_proxy", false),
                entry("residential_proxy", false),
                entry("domain", "frpt.net"),
                entry("isp", "Fairpoint Communications"),
                entry("isp_organization_name", "Fairpoint Communications"),
                entry("user_type", "residential"),
                entry("connection_type", "Cable/DSL"),
                entry("registered_country_in_european_union", false),
                entry("registered_country_iso_code", "US"),
                entry("registered_country_name", "United States")
            )
        );
    }

    public void testIsp() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "GeoIP2-ISP-Test.mmdb";
        String ip = "149.101.100.1";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new MaxmindIpDataLookups.Isp(Database.Isp.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("asn", 6167L),
                entry("organization_name", "CELLCO-PART"),
                entry("network", "149.101.100.0/28"),
                entry("isp", "Verizon Wireless"),
                entry("isp_organization_name", "Verizon Wireless"),
                entry("mobile_network_code", "004"),
                entry("mobile_country_code", "310")
            )
        );
    }

    public void testDatabaseTypeParsing() throws IOException {
        // this test is a little bit overloaded -- it's testing that we're getting the expected sorts of
        // database_type strings from these files, *and* it's also testing that we dispatch on those strings
        // correctly and associated those files with the correct high-level Elasticsearch Database type.
        // down the road it would probably make sense to split these out and find a better home for some of the
        // logic, but for now it's probably more valuable to have the test *somewhere* than to get especially
        // pedantic about where precisely it should be.

        copyDatabase("GeoLite2-City-Test.mmdb", tmpDir);
        copyDatabase("GeoLite2-Country-Test.mmdb", tmpDir);
        copyDatabase("GeoLite2-ASN-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Anonymous-IP-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-City-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Country-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Connection-Type-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Domain-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-Enterprise-Test.mmdb", tmpDir);
        copyDatabase("GeoIP2-ISP-Test.mmdb", tmpDir);

        assertThat(parseDatabaseFromType("GeoLite2-City-Test.mmdb"), is(Database.City));
        assertThat(parseDatabaseFromType("GeoLite2-Country-Test.mmdb"), is(Database.Country));
        assertThat(parseDatabaseFromType("GeoLite2-ASN-Test.mmdb"), is(Database.Asn));
        assertThat(parseDatabaseFromType("GeoIP2-Anonymous-IP-Test.mmdb"), is(Database.AnonymousIp));
        assertThat(parseDatabaseFromType("GeoIP2-City-Test.mmdb"), is(Database.City));
        assertThat(parseDatabaseFromType("GeoIP2-Country-Test.mmdb"), is(Database.Country));
        assertThat(parseDatabaseFromType("GeoIP2-Connection-Type-Test.mmdb"), is(Database.ConnectionType));
        assertThat(parseDatabaseFromType("GeoIP2-Domain-Test.mmdb"), is(Database.Domain));
        assertThat(parseDatabaseFromType("GeoIP2-Enterprise-Test.mmdb"), is(Database.Enterprise));
        assertThat(parseDatabaseFromType("GeoIP2-ISP-Test.mmdb"), is(Database.Isp));
    }

    private Database parseDatabaseFromType(String databaseFile) throws IOException {
        return IpDataLookupFactories.getDatabase(MMDBUtil.getDatabaseType(tmpDir.resolve(databaseFile)));
    }

    private void assertExpectedLookupResults(String databaseName, String ip, IpDataLookup lookup, Map<String, Object> expected) {
        try (DatabaseReaderLazyLoader loader = loader(databaseName)) {
            Map<String, Object> actual = lookup.getData(loader, ip);
            assertThat(
                "The set of keys in the result are not the same as the set of expected keys",
                actual.keySet(),
                containsInAnyOrder(expected.keySet().toArray(new String[0]))
            );
            for (Map.Entry<String, Object> entry : expected.entrySet()) {
                assertThat("Unexpected value for key [" + entry.getKey() + "]", actual.get(entry.getKey()), equalTo(entry.getValue()));
            }
        } catch (AssertionError e) {
            fail(e, "Assert failed for database [%s] with address [%s]", databaseName, ip);
        } catch (Exception e) {
            fail(e, "Exception for database [%s] with address [%s]", databaseName, ip);
        }
    }

    private DatabaseReaderLazyLoader loader(final String databaseName) {
        Path path = tmpDir.resolve(databaseName);
        copyDatabase(databaseName, path);
        final GeoIpCache cache = GeoIpCache.createGeoIpCacheWithMaxCount(1000);
        return new DatabaseReaderLazyLoader(cache, path, null);
    }
}
