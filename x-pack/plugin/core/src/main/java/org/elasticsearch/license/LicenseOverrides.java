/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.common.hash.MessageDigests;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;

/**
 * Contains license expiration date overrides.
 */
class LicenseOverrides {
    private LicenseOverrides() {}

    // This approach of just having everything in a hardcoded map is based on an assumption that we will need to do this very infrequently.
    // If this assumption proves incorrect, we should consider switching to another approach,
    private static final Map<String, ZonedDateTime> LICENSE_OVERRIDES;

    static {
        // This value is not a "real" license ID, it is used for testing this code
        String TEST_LICENSE_ID_HASH = MessageDigests.toHexString(
            MessageDigests.sha256().digest("12345678-abcd-0000-0000-000000000000".getBytes(StandardCharsets.UTF_8))
        );

        LICENSE_OVERRIDES = Map.ofEntries(
            Map.entry(
                TEST_LICENSE_ID_HASH,
                ZonedDateTime.ofStrict(LocalDateTime.of(1970, 1, 1, 0, 0, 42, 0), ZoneOffset.UTC, ZoneOffset.UTC)
            )
        );
    }

    static Optional<ZonedDateTime> overrideDateForLicense(String licenseUidHash) {
        return Optional.ofNullable(LICENSE_OVERRIDES.get(licenseUidHash));
    }
}
