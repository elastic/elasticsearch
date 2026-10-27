/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.elasticsearch.common.time.DateUtils;

public class DateSyntheticSourceNativeArrayIntegrationTests extends NativeArrayIntegrationTestCase {

    @Override
    protected String getFieldTypeName() {
        return "date";
    }

    @Override
    protected Object getRandomValue() {
        // Format with the default formatter so synthetic source reproduces the input exactly.
        return DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(randomLongBetween(0, DateUtils.MAX_MILLIS_BEFORE_9999));
    }

    @Override
    protected Object getMalformedValue() {
        // Letters only, which neither strict_date_optional_time nor epoch_millis accept.
        return RandomStrings.randomAsciiLettersOfLength(random(), 8);
    }
}
