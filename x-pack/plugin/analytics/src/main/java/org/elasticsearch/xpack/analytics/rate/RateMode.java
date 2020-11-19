/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.rate;

import java.util.Locale;

/**
 * Rate mode - value_count or sum
 */
public enum RateMode {
    VALUE_COUNT, SUM;

    public static RateMode resolve(String name) {
        return RateMode.valueOf(name.toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

}
