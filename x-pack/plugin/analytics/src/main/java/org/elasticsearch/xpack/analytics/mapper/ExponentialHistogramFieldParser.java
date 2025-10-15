/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.util.FeatureFlag;

public class ExponentialHistogramFieldParser {

    public static final FeatureFlag EXPONENTIAL_HISTOGRAM_FEATURE = new FeatureFlag("exponential_histogram");

    public static void isExponentialHistogramFieldMember(String jsonFieldName) {

    }
}
