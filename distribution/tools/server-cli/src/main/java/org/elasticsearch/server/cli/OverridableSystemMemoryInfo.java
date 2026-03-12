/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import java.util.List;
import java.util.Objects;

/**
 * A {@link SystemMemoryInfo} which returns a user-overridden memory size if one
 * has been specified using the {@code es.total_memory_bytes} system property, or
 * else returns the value provided by a fallback provider.
 */
public final class OverridableSystemMemoryInfo extends JvmArgumentParsingSystemMemoryInfo {

    private final SystemMemoryInfo fallbackSystemMemoryInfo;

    public OverridableSystemMemoryInfo(final List<String> userDefinedJvmOptions, SystemMemoryInfo fallbackSystemMemoryInfo) {
        super(userDefinedJvmOptions);
        this.fallbackSystemMemoryInfo = Objects.requireNonNull(fallbackSystemMemoryInfo);
    }

    @Override
    public long availableSystemMemory() {
        return getBytesFromSystemProperty("es.total_memory_bytes", fallbackSystemMemoryInfo.availableSystemMemory());
    }
}
