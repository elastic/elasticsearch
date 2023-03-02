/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.analytics;

import org.elasticsearch.test.ESTestCase;

public final class AnalyticsTestUtils {

    private AnalyticsTestUtils() {
        throw new UnsupportedOperationException("Dont instantiate this class");
    }

    public static AnalyticsCollection randomAnalyticsCollection() {
        return new AnalyticsCollection(ESTestCase.randomAlphaOfLengthBetween(1, 10));
    }
}
