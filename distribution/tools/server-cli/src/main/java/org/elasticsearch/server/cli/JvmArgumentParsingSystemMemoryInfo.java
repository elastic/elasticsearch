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

public abstract class JvmArgumentParsingSystemMemoryInfo implements SystemMemoryInfo {
    private final List<String> userDefinedJvmOptions;

    public JvmArgumentParsingSystemMemoryInfo(List<String> userDefinedJvmOptions) {
        this.userDefinedJvmOptions = userDefinedJvmOptions;
    }

    protected long getBytesFromSystemProperty(String systemProperty, long defaultValue) {
        return userDefinedJvmOptions.stream()
            .filter(option -> option.startsWith("-D" + systemProperty + "="))
            .map(totalMemoryOverheadBytesOption -> {
                try {
                    long bytes = Long.parseLong(totalMemoryOverheadBytesOption.split("=", 2)[1]);
                    if (bytes < 0) {
                        throw new IllegalArgumentException("Negative bytes size specified in [" + totalMemoryOverheadBytesOption + "]");
                    }
                    return bytes;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Unable to parse number of bytes from [" + totalMemoryOverheadBytesOption + "]", e);
                }
            })
            .reduce((previous, current) -> current) // this is effectively findLast(), so that ES_JAVA_OPTS overrides jvm.options
            .orElse(defaultValue);
    }
}
