/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.ttest;

import java.util.Locale;

/**
 * T-test type, paired, unpaired equal variance, unpaired unequal variance
 */
public enum TTestType {
    PAIRED,
    HOMOSCEDASTIC,
    HETEROSCEDASTIC;

    public static TTestType resolve(String name) {
        return TTestType.valueOf(name.toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

}
