/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.DatabaseRecord;
import com.maxmind.db.Networks;
import com.maxmind.db.Reader;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Map.entry;
import static org.elasticsearch.ingest.geoip.GeoIpTestUtils.copyDatabase;
import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.ipinfoTypeCleanup;
import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.parseAsn;
import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.parseBoolean;
import static org.elasticsearch.ingest.geoip.IpinfoIpDataLookups.parseLocationDouble;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class IpinfoIpDataLookupsTests extends ESTestCase {

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

    public void testParseAsn() {
        // expected case: "AS123" is 123
        assertThat(parseAsn("AS123"), equalTo(123L));
        // defensive cases: null and empty becomes null, this is not expected fwiw
        assertThat(parseAsn(null), nullValue());
        assertThat(parseAsn(""), nullValue());
        // defensive cases: we strip whitespace and ignore case
        assertThat(parseAsn(" as 456  "), equalTo(456L));
        // defensive cases: we ignore the absence of the 'AS' prefix
        assertThat(parseAsn("123"), equalTo(123L));
        // bottom case: a non-parsable string is null
        assertThat(parseAsn("anythingelse"), nullValue());
    }

    public void testParseBoolean() {
        // expected cases: "true" is true and "" is false
        assertThat(parseBoolean("true"), equalTo(true));
        assertThat(parseBoolean(""), equalTo(false));
        assertThat(parseBoolean("false"), equalTo(false)); // future proofing
        // defensive case: null becomes null, this is not expected fwiw
        assertThat(parseBoolean(null), nullValue());
        // defensive cases: we strip whitespace and ignore case
        assertThat(parseBoolean("    "), equalTo(false));
        assertThat(parseBoolean(" TrUe "), equalTo(true));
        assertThat(parseBoolean(" FaLSE "), equalTo(false));
        // bottom case: a non-parsable string is null
        assertThat(parseBoolean(randomAlphaOfLength(8)), nullValue());
    }

    public void testParseLocationDouble() {
        // expected case: "123.45" is 123.45
        assertThat(parseLocationDouble("123.45"), equalTo(123.45));
        // defensive cases: null and empty becomes null, this is not expected fwiw
        assertThat(parseLocationDouble(null), nullValue());
        assertThat(parseLocationDouble(""), nullValue());
        // defensive cases: we strip whitespace
        assertThat(parseLocationDouble("  -123.45  "), equalTo(-123.45));
        // bottom case: a non-parsable string is null
        assertThat(parseLocationDouble("anythingelse"), nullValue());
    }

    public void testAsnFree() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "ip_asn_sample.mmdb";
        String ip = "23.32.184.0";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new IpinfoIpDataLookups.Asn(Database.AsnV2.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("organization_name", "Akamai Technologies, Inc."),
                entry("asn", 16625L),
                entry("network", "23.32.184.0/21"),
                entry("domain", "akamai.com")
            )
        );
    }

    public void testAsnStandard() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "asn_sample.mmdb";
        String ip = "69.19.224.0";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new IpinfoIpDataLookups.Asn(Database.AsnV2.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("organization_name", "TPx Communications"),
                entry("asn", 14265L),
                entry("network", "69.19.224.0/22"),
                entry("domain", "tpx.com"),
                entry("type", "hosting"),
                entry("country_iso_code", "US")
            )
        );
    }

    public void testAsnInvariants() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        Path configDir = tmpDir;
        copyDatabase("ipinfo/ip_asn_sample.mmdb", configDir.resolve("ip_asn_sample.mmdb"));
        copyDatabase("ipinfo/asn_sample.mmdb", configDir.resolve("asn_sample.mmdb"));

        {
            final Set<String> expectedColumns = Set.of("network", "asn", "name", "domain");

            Path databasePath = configDir.resolve("ip_asn_sample.mmdb");
            assertDatabaseInvariants(databasePath, (ip, row) -> {
                assertThat(row.keySet(), equalTo(expectedColumns));
                String asn = (String) row.get("asn");
                assertThat(asn, startsWith("AS"));
                assertThat(asn, equalTo(asn.trim()));
                Long parsed = parseAsn(asn);
                assertThat(parsed, notNullValue());
                assertThat(asn, equalTo("AS" + parsed)); // reverse it
            });
        }

        {
            final Set<String> expectedColumns = Set.of("network", "asn", "name", "domain", "country", "type");

            Path databasePath = configDir.resolve("asn_sample.mmdb");
            assertDatabaseInvariants(databasePath, (ip, row) -> {
                assertThat(row.keySet(), equalTo(expectedColumns));
                String asn = (String) row.get("asn");
                assertThat(asn, startsWith("AS"));
                assertThat(asn, equalTo(asn.trim()));
                Long parsed = parseAsn(asn);
                assertThat(parsed, notNullValue());
                assertThat(asn, equalTo("AS" + parsed)); // reverse it
            });
        }
    }

    public void testCountryFree() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "ip_country_sample.mmdb";
        String ip = "20.33.76.0";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new IpinfoIpDataLookups.Country(Database.CountryV2.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("country_name", "Ireland"),
                entry("country_iso_code", "IE"),
                entry("continent_name", "Europe"),
                entry("continent_code", "EU")
            )
        );
    }

    public void testGeolocationStandard() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "ip_geolocation_standard_sample.mmdb";
        String ip = "62.69.48.19";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new IpinfoIpDataLookups.Geolocation(Database.CityV2.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("country_iso_code", "GB"),
                entry("region_name", "England"),
                entry("city_name", "London"),
                entry("timezone", "Europe/London"),
                entry("postal_code", "E1W"),
                entry("location", Map.of("lat", 51.50853, "lon", -0.12574))
            )
        );
    }

    public void testGeolocationInvariants() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        Path configDir = tmpDir;
        copyDatabase("ipinfo/ip_geolocation_standard_sample.mmdb", configDir.resolve("ip_geolocation_standard_sample.mmdb"));

        {
            final Set<String> expectedColumns = Set.of(
                "city",
                "geoname_id",
                "region",
                "region_code",
                "country",
                "postal_code",
                "timezone",
                "lat",
                "lng"
            );

            Path databasePath = configDir.resolve("ip_geolocation_standard_sample.mmdb");
            assertDatabaseInvariants(databasePath, (ip, row) -> {
                assertThat(row.keySet(), equalTo(expectedColumns));
                {
                    String latitude = (String) row.get("lat");
                    assertThat(latitude, equalTo(latitude.trim()));
                    Double parsed = parseLocationDouble(latitude);
                    assertThat(parsed, notNullValue());
                    assertThat(Double.parseDouble(latitude), equalTo(Double.parseDouble(Double.toString(parsed)))); // reverse it
                }
                {
                    String longitude = (String) row.get("lng");
                    assertThat(longitude, equalTo(longitude.trim()));
                    Double parsed = parseLocationDouble(longitude);
                    assertThat(parsed, notNullValue());
                    assertThat(Double.parseDouble(longitude), equalTo(Double.parseDouble(Double.toString(parsed)))); // reverse it
                }
            });
        }
    }

    public void testPrivacyDetectionStandard() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "privacy_detection_sample.mmdb";
        String ip = "2.57.109.154";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new IpinfoIpDataLookups.PrivacyDetection(Database.PrivacyDetection.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("hosting", false),
                entry("proxy", false),
                entry("relay", false),
                entry("tor", false),
                entry("vpn", true)
            )
        );
    }

    public void testPrivacyDetectionStandardNonEmptyService() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        String databaseName = "privacy_detection_sample.mmdb";
        String ip = "59.29.201.246";
        assertExpectedLookupResults(
            databaseName,
            ip,
            new IpinfoIpDataLookups.PrivacyDetection(Database.PrivacyDetection.properties()),
            Map.ofEntries(
                entry("ip", ip),
                entry("hosting", false),
                entry("proxy", false),
                entry("service", "VPNGate"),
                entry("relay", false),
                entry("tor", false),
                entry("vpn", true)
            )
        );
    }

    public void testPrivacyDetectionInvariants() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/114266", Constants.WINDOWS);
        Path configDir = tmpDir;
        copyDatabase("ipinfo/privacy_detection_sample.mmdb", configDir.resolve("privacy_detection_sample.mmdb"));

        {
            final Set<String> expectedColumns = Set.of("network", "service", "hosting", "proxy", "relay", "tor", "vpn");

            Path databasePath = configDir.resolve("privacy_detection_sample.mmdb");
            assertDatabaseInvariants(databasePath, (ip, row) -> {
                assertThat(row.keySet(), equalTo(expectedColumns));

                for (String booleanColumn : Set.of("hosting", "proxy", "relay", "tor", "vpn")) {
                    String bool = (String) row.get(booleanColumn);
                    assertThat(bool, anyOf(equalTo("true"), equalTo(""), equalTo("false")));
                    assertThat(parseBoolean(bool), notNullValue());
                }
            });
        }
    }

    public void testIpinfoTypeCleanup() {
        Map<String, String> typesToCleanedTypes = Map.ofEntries(
            // database_type strings from upstream:
            // abuse.mmdb
            entry("ipinfo standard_abuse_mmdb_v4.mmdb", "abuse_v4"),
            // asn.mmdb
            entry("ipinfo generic_asn_mmdb_v4.mmdb", "asn_v4"),
            // carrier.mmdb
            entry("ipinfo standard_carrier_mmdb.mmdb", "carrier"),
            // location_extended_v2.mmdb
            entry("ipinfo extended_location_v2.mmdb", "location_v2"),
            // privacy_extended_v2.mmdb
            entry("ipinfo extended_privacy_v2.mmdb", "privacy_v2"),
            // standard_company.mmdb
            entry("ipinfo standard_company.mmdb", "company"),
            // standard_ip_hosted_domains_sample.mmdb
            entry("ipinfo standard_ip_hosted_domains_sample.mmdb", "hosted_domains"),
            // standard_location.mmdb
            entry("ipinfo standard_location_mmdb_v4.mmdb", "location_v4"),
            // standard_privacy.mmdb
            entry("ipinfo standard_privacy.mmdb", "privacy"),

            // database_type strings from test files:
            // ip_asn_sample.mmdb
            entry("ipinfo ip_asn_sample.mmdb", "asn"),
            // ip_country_asn_sample.mmdb
            entry("ipinfo ip_country_asn_sample.mmdb", "country_asn"),
            // ip_geolocation_sample.mmdb
            entry("ipinfo ip_geolocation_sample.mmdb", "geolocation"),
            // abuse_contact_sample.mmdb
            entry("ipinfo abuse_contact_sample.mmdb", "abuse_contact"),
            // asn_sample.mmdb
            entry("ipinfo asn_sample.mmdb", "asn"),
            // hosted_domains_sample.mmdb
            entry("ipinfo hosted_domains_sample.mmdb", "hosted_domains"),
            // ip_carrier_sample.mmdb
            entry("ipinfo ip_carrier_sample.mmdb", "carrier"),
            // ip_company_sample.mmdb
            entry("ipinfo ip_company_sample.mmdb", "company"),
            // ip_country_sample.mmdb
            entry("ipinfo ip_country_sample.mmdb", "country"),
            // ip_geolocation_extended_ipv4_sample.mmdb
            entry("ipinfo ip_geolocation_extended_ipv4_sample.mmdb", "geolocation_ipv4"),
            // ip_geolocation_extended_ipv6_sample.mmdb
            entry("ipinfo ip_geolocation_extended_ipv6_sample.mmdb", "geolocation_ipv6"),
            // ip_geolocation_extended_sample.mmdb
            entry("ipinfo ip_geolocation_extended_sample.mmdb", "geolocation"),
            // ip_rdns_domains_sample.mmdb
            entry("ipinfo ip_rdns_domains_sample.mmdb", "rdns_domains"),
            // ip_rdns_hostnames_sample.mmdb
            entry("ipinfo ip_rdns_hostnames_sample.mmdb", "rdns_hostnames"),
            // privacy_detection_extended_sample.mmdb
            entry("ipinfo privacy_detection_extended_sample.mmdb", "privacy_detection"),
            // privacy_detection_sample.mmdb
            entry("ipinfo privacy_detection_sample.mmdb", "privacy_detection"),

            // database_type strings from downloaded (free) files:
            // asn.mmdb
            entry("ipinfo generic_asn_free.mmdb", "asn"),
            // country.mmdb
            entry("ipinfo generic_country_free.mmdb", "country"),
            // country_asn.mmdb
            entry("ipinfo generic_country_free_country_asn.mmdb", "country_country_asn")
        );

        for (var entry : typesToCleanedTypes.entrySet()) {
            String type = entry.getKey();
            String cleanedType = entry.getValue();
            assertThat(ipinfoTypeCleanup(type), equalTo(cleanedType));
        }
    }

    public void testDatabaseTypeParsing() throws IOException {
        // this test is a little bit overloaded -- it's testing that we're getting the expected sorts of
        // database_type strings from these files, *and* it's also testing that we dispatch on those strings
        // correctly and associated those files with the correct high-level Elasticsearch Database type.
        // down the road it would probably make sense to split these out and find a better home for some of the
        // logic, but for now it's probably more valuable to have the test *somewhere* than to get especially
        // pedantic about where precisely it should be.

        copyDatabase("ipinfo/ip_asn_sample.mmdb", tmpDir.resolve("ip_asn_sample.mmdb"));
        copyDatabase("ipinfo/ip_geolocation_standard_sample.mmdb", tmpDir.resolve("ip_geolocation_standard_sample.mmdb"));
        copyDatabase("ipinfo/asn_sample.mmdb", tmpDir.resolve("asn_sample.mmdb"));
        copyDatabase("ipinfo/ip_country_sample.mmdb", tmpDir.resolve("ip_country_sample.mmdb"));
        copyDatabase("ipinfo/privacy_detection_sample.mmdb", tmpDir.resolve("privacy_detection_sample.mmdb"));

        assertThat(parseDatabaseFromType("ip_asn_sample.mmdb"), is(Database.AsnV2));
        assertThat(parseDatabaseFromType("ip_geolocation_standard_sample.mmdb"), is(Database.CityV2));
        assertThat(parseDatabaseFromType("asn_sample.mmdb"), is(Database.AsnV2));
        assertThat(parseDatabaseFromType("ip_country_sample.mmdb"), is(Database.CountryV2));
        assertThat(parseDatabaseFromType("privacy_detection_sample.mmdb"), is(Database.PrivacyDetection));

        // additional cases where we're bailing early on types we don't support
        assertThat(IpDataLookupFactories.getDatabase("ipinfo ip_country_asn_sample.mmdb"), nullValue());
        assertThat(IpDataLookupFactories.getDatabase("ipinfo privacy_detection_extended_sample.mmdb"), nullValue());
    }

    private Database parseDatabaseFromType(String databaseFile) throws IOException {
        return IpDataLookupFactories.getDatabase(MMDBUtil.getDatabaseType(tmpDir.resolve(databaseFile)));
    }

    private static void assertDatabaseInvariants(final Path databasePath, final BiConsumer<InetAddress, Map<String, Object>> rowConsumer) {
        try (Reader reader = new Reader(pathToFile(databasePath))) {
            Networks<?> networks = reader.networks(Map.class);
            while (networks.hasNext()) {
                DatabaseRecord<?> dbr = networks.next();
                InetAddress address = dbr.getNetwork().getNetworkAddress();
                @SuppressWarnings("unchecked")
                Map<String, Object> result = reader.get(address, Map.class);
                try {
                    rowConsumer.accept(address, result);
                } catch (AssertionError e) {
                    fail(e, "Assert failed for address [%s]", NetworkAddress.format(address));
                } catch (Exception e) {
                    fail(e, "Exception handling address [%s]", NetworkAddress.format(address));
                }
            }
        } catch (Exception e) {
            fail(e);
        }
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private static File pathToFile(Path databasePath) {
        return databasePath.toFile();
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
        copyDatabase("ipinfo/" + databaseName, path); // the ipinfo databases are prefixed on the test classpath
        final GeoIpCache cache = new GeoIpCache(1000);
        return new DatabaseReaderLazyLoader(cache, path, null);
    }
}
