/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import java.util.Locale;

public enum ResultDiversificationType {
    MMR("mmr");

    public final String value;

    ResultDiversificationType(String value) {
        this.value = value;
    }

    public static ResultDiversificationType fromString(String value) {
        for (ResultDiversificationType diversificationType : ResultDiversificationType.values()) {
            if (diversificationType.value.equalsIgnoreCase(value)) {
                return diversificationType;
            }
        }

        throw new IllegalArgumentException(String.format(Locale.ROOT, "unknown result diversification type [%s]", value));
    }
}
