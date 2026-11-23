/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.aws;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

/**
 * Lazy supplier for a region name. We cannot use randomness like {@link ESTestCase#randomIdentifier()} when creating the test fixtures in
 * the first place because this happens in static context, so instead we create one of these and defer the creation of the region name
 * itself until the test actually starts running.
 */
public class DynamicRegionSupplier extends DynamicIdentifierSupplier {
    public DynamicRegionSupplier() {
        super(() -> "DynamicRegionSupplier-");
    }

    /**
     * Region names containing "fips" make the AWS SDK select FIPS endpoints, which the test fixtures do not serve.
     */
    @Override
    protected boolean isInvalidIdentifier(String value) {
        return value.toLowerCase(Locale.ROOT).contains("fips");
    }
}
