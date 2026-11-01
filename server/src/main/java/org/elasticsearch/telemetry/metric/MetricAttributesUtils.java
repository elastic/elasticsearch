/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import java.util.Locale;

public class MetricAttributesUtils {

    private MetricAttributesUtils() {}

    /**
     * Normalizes the product origin value by lowercasing it and checking against known origins. If the value is not recognized,
     * returns the provided default value.
     * @param value the product origin value to normalize
     * @param defaultValue the default value to return if the input value is not a known product origin
     * @return the normalized product origin if recognized, otherwise the default value
     */
    public static String normalizeProductOrigin(String value, String defaultValue) {
        var lowered = value.toLowerCase(Locale.ROOT);
        return MetricAttributes.KNOWN_PRODUCT_ORIGINS.contains(lowered) ? lowered : defaultValue;
    }
}
