/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.config;

import java.util.Locale;

public enum BlockReason {
    DELETE, RESET, REVERT;

    public static BlockReason fromString(String value) {
        return BlockReason.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
