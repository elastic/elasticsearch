/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

import java.util.LinkedHashMap;
import java.util.SequencedMap;

/**
 * A canonical, documented mapping from an observable ip location database file name shape (expressed as a glob
 * pattern) to the database variant it corresponds to. MaxMind databases are matched by filename suffix (reliable);
 * ipinfo databases are matched heuristically by substring, since ipinfo file names are not standardized.
 * <p>
 * This table exists to be shared by consumers that need to describe or reason about known database file names
 * without depending on the actual resolution logic in {@code modules/ip-location} (e.g. ES|QL's Kibana docs
 * generation, which renders one output variant per glob). It is validated against the real resolution logic by a
 * roundtrip test in {@code modules/ip-location}, which builds an example file name for each glob and asserts it
 * resolves to the claimed variant through the actual (package-private) resolver.
 * <p>
 * Values are the variant's {@code Database} enum name (e.g. {@code "City"}, {@code "AsnV2"}) as a plain
 * {@link String}, not the {@code Database} enum type itself, because that enum lives in {@code modules/ip-location},
 * which depends on this library, not the other way around: {@code libs/ip-location-api} cannot see it.
 */
public final class IpDatabaseFileGlobs {

    private IpDatabaseFileGlobs() {}

    /**
     * Glob patterns for known ip location database file names, in the order they should be rendered, mapped to the
     * name of the {@code Database} variant (in {@code modules/ip-location}) they correspond to.
     */
    public static final SequencedMap<String, String> DATABASE_VARIANT_GLOBS = new LinkedHashMap<>();
    static {
        DATABASE_VARIANT_GLOBS.put("*-City.mmdb", "City");
        DATABASE_VARIANT_GLOBS.put("*-Country.mmdb", "Country");
        DATABASE_VARIANT_GLOBS.put("*-ASN.mmdb", "Asn");
        DATABASE_VARIANT_GLOBS.put("*-Anonymous-IP.mmdb", "AnonymousIp");
        DATABASE_VARIANT_GLOBS.put("*-Connection-Type.mmdb", "ConnectionType");
        DATABASE_VARIANT_GLOBS.put("*-Domain.mmdb", "Domain");
        DATABASE_VARIANT_GLOBS.put("*-Enterprise.mmdb", "Enterprise");
        DATABASE_VARIANT_GLOBS.put("*-ISP.mmdb", "Isp");
        DATABASE_VARIANT_GLOBS.put("ipinfo*plus*.mmdb", "IpinfoPlus");
        DATABASE_VARIANT_GLOBS.put("ipinfo*asn*.mmdb", "AsnV2");
        DATABASE_VARIANT_GLOBS.put("ipinfo*country*.mmdb", "CountryV2");
        DATABASE_VARIANT_GLOBS.put("ipinfo*location*.mmdb", "CityV2");
        DATABASE_VARIANT_GLOBS.put("ipinfo*privacy*.mmdb", "PrivacyDetection");
    }
}
