/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.iplocation.api.IpDatabaseFileGlobs;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that the canonical filename-glob-to-variant table in {@link IpDatabaseFileGlobs} (consumed by ES|QL's
 * Kibana docs generation) actually matches the real, production filename resolution logic in
 * {@link IpDataLookupFactories}. For each glob, this builds one concrete example file name satisfying the glob and
 * confirms it resolves to the claimed {@link Database} variant.
 */
public class IpDatabaseFileGlobsTests extends ESTestCase {

    // one concrete, glob-satisfying example filename per entry in IpDatabaseFileGlobs.DATABASE_VARIANT_GLOBS
    private static final Map<String, String> EXAMPLE_FILE_NAMES = Map.ofEntries(
        Map.entry("*-City.mmdb", "GeoLite2-City.mmdb"),
        Map.entry("*-Country.mmdb", "GeoLite2-Country.mmdb"),
        Map.entry("*-ASN.mmdb", "GeoLite2-ASN.mmdb"),
        Map.entry("*-Anonymous-IP.mmdb", "GeoIP2-Anonymous-IP.mmdb"),
        Map.entry("*-Connection-Type.mmdb", "GeoIP2-Connection-Type.mmdb"),
        Map.entry("*-Domain.mmdb", "GeoIP2-Domain.mmdb"),
        Map.entry("*-Enterprise.mmdb", "GeoIP2-Enterprise.mmdb"),
        Map.entry("*-ISP.mmdb", "GeoIP2-ISP.mmdb"),
        Map.entry("ipinfo*plus*.mmdb", "ipinfo_location_plus.mmdb"),
        Map.entry("ipinfo*asn*.mmdb", "ipinfo_asn.mmdb"),
        Map.entry("ipinfo*country*.mmdb", "ipinfo_country.mmdb"),
        Map.entry("ipinfo*location*.mmdb", "ipinfo_location.mmdb"),
        Map.entry("ipinfo*privacy*.mmdb", "ipinfo_privacy_detection.mmdb")
    );

    public void testEveryGlobExampleResolvesToItsClaimedVariant() {
        assertThat(
            "test's example file names must cover every glob in IpDatabaseFileGlobs.DATABASE_VARIANT_GLOBS",
            EXAMPLE_FILE_NAMES.keySet(),
            equalTo(IpDatabaseFileGlobs.DATABASE_VARIANT_GLOBS.keySet())
        );

        for (Map.Entry<String, String> entry : IpDatabaseFileGlobs.DATABASE_VARIANT_GLOBS.entrySet()) {
            String glob = entry.getKey();
            String variantName = entry.getValue();
            String fileName = EXAMPLE_FILE_NAMES.get(glob);
            assertThat("missing example file name for glob [" + glob + "]", fileName, notNullValue());

            String databaseType = IpDataLookupFactories.guessDatabaseType(fileName);
            Database resolved = IpDataLookupFactories.getDatabase(databaseType);
            assertThat(
                "file name [" + fileName + "] for glob [" + glob + "] should resolve to database variant [" + variantName + "]",
                resolved,
                equalTo(Database.valueOf(variantName))
            );
        }
    }
}
