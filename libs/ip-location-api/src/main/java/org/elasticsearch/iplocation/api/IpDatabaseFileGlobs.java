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
 * Canonical mapping from ip location database file name to the database variant
 * it corresponds to.
 * <p>
 *    MaxMind databases are matched by filename suffix which is fairly standardized.
 *    ipinfo databases are matched heuristically by substring. And those aren't
 *    particularly standardized.
 * </p>
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
