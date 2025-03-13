/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import java.util.Locale;

public enum FilteringPolicy {
    EXCLUDE,
    INCLUDE;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static FilteringPolicy filteringPolicy(String policy) {
        for (FilteringPolicy filteringPolicy : FilteringPolicy.values()) {
            if (filteringPolicy.name().equalsIgnoreCase(policy)) {
                return filteringPolicy;
            }
        }
        throw new IllegalArgumentException("Unknown " + FilteringPolicy.class.getSimpleName() + " [" + policy + "].");
    }
}
