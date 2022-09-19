/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analytics.ttest;

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
