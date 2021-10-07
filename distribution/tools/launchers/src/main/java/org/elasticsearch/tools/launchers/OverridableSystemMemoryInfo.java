/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import java.util.List;
import java.util.Objects;

/**
 * A {@link SystemMemoryInfo} which returns a user-overridden memory size if one
 * has been specified using the {@code es.total_memory_bytes} system property, or
 * else returns the value provided by a fallback provider.
 */
public final class OverridableSystemMemoryInfo implements SystemMemoryInfo {

    private final List<String> userDefinedJvmOptions;
    private final SystemMemoryInfo fallbackSystemMemoryInfo;

    public OverridableSystemMemoryInfo(final List<String> userDefinedJvmOptions, SystemMemoryInfo fallbackSystemMemoryInfo) {
        this.userDefinedJvmOptions = Objects.requireNonNull(userDefinedJvmOptions);
        this.fallbackSystemMemoryInfo = Objects.requireNonNull(fallbackSystemMemoryInfo);
    }

    @Override
    public long availableSystemMemory() throws SystemMemoryInfoException {

        return userDefinedJvmOptions.stream()
            .filter(option -> option.startsWith("-Des.total_memory_bytes="))
            .map(totalMemoryBytesOption -> {
                try {
                    long bytes = Long.parseLong(totalMemoryBytesOption.split("=", 2)[1]);
                    if (bytes < 0) {
                        throw new IllegalArgumentException("Negative memory size specified in [" + totalMemoryBytesOption + "]");
                    }
                    return bytes;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Unable to parse number of bytes from [" + totalMemoryBytesOption + "]", e);
                }
            })
            .reduce((previous, current) -> current) // this is effectively findLast(), so that ES_JAVA_OPTS overrides jvm.options
            .orElse(fallbackSystemMemoryInfo.availableSystemMemory());
    }
}
