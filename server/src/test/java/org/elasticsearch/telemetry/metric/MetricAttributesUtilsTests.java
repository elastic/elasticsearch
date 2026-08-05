/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.metric;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;

import static org.elasticsearch.telemetry.metric.MetricAttributesUtils.normalizeProductOrigin;
import static org.hamcrest.Matchers.equalTo;

public class MetricAttributesUtilsTests extends ESTestCase {

    private static final String DEFAULT_VALUE = "other";
    private static final String KNOWN_ORIGIN = "kibana";

    public void testKnownOrigin_ExactMatch_ReturnsValue() {
        assertThat(normalizeProductOrigin(KNOWN_ORIGIN, DEFAULT_VALUE), equalTo(KNOWN_ORIGIN));
    }

    public void testKnownOrigin_UpperCase_NormalizedToLowercase() {
        assertThat(normalizeProductOrigin("KIBANA", DEFAULT_VALUE), equalTo(KNOWN_ORIGIN));
    }

    public void testKnownOrigin_MixedCase_NormalizedToLowercase() {
        assertThat(normalizeProductOrigin("Kibana", DEFAULT_VALUE), equalTo(KNOWN_ORIGIN));
    }

    public void testUnknownOrigin_ReturnsDefaultValue() {
        var defaultValue = randomAlphaOfLength(10);
        assertThat(normalizeProductOrigin("bogus-origin", defaultValue), equalTo(defaultValue));
    }

    public void testAllKnownOriginsAreRecognized() {
        for (var origin : MetricAttributes.KNOWN_PRODUCT_ORIGINS) {
            assertThat(
                "KNOWN_PRODUCT_ORIGINS entry [" + origin + "] was not recognized by normalizeProductOrigin",
                normalizeProductOrigin(origin, DEFAULT_VALUE),
                equalTo(origin)
            );
        }
    }

    public void testAllKnownOriginsAreLowercase() {
        for (var origin : MetricAttributes.KNOWN_PRODUCT_ORIGINS) {
            assertThat("KNOWN_PRODUCT_ORIGINS entry must be lowercase: " + origin, origin, equalTo(origin.toLowerCase(Locale.ROOT)));
        }
    }
}
