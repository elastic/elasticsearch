/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import java.util.Locale;

public enum FilteringValidationState {
    EDITED,
    INVALID,
    VALID;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static FilteringValidationState filteringValidationState(String validationState) {
        for (FilteringValidationState filteringValidationState : FilteringValidationState.values()) {
            if (filteringValidationState.name().equalsIgnoreCase(validationState)) {
                return filteringValidationState;
            }
        }
        throw new IllegalArgumentException("Unknown " + FilteringValidationState.class.getSimpleName() + " [" + validationState + "].");
    }
}
