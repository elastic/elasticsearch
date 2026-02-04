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

/**
 * A {@link SystemMemoryInfo} implementation that reduces the reported available system memory by a set amount. This is intended to account
 * for overhead cost of other bundled Elasticsearch processes, such as the CLI tool launcher. By default, this is
 * {@link org.elasticsearch.server.cli.OverheadSystemMemoryInfo#SERVER_CLI_OVERHEAD } but can be overridden via the
 * {@code es.total_memory_overhead_bytes} system property.
 */
public class OverheadSystemMemoryInfo extends JvmArgumentParsingSystemMemoryInfo {
    static final long SERVER_CLI_OVERHEAD = 100 * 1024L * 1024L;

    private final SystemMemoryInfo delegate;

    public OverheadSystemMemoryInfo(List<String> userDefinedJvmOptions, SystemMemoryInfo delegate) {
        super(userDefinedJvmOptions);
        this.delegate = delegate;
    }

    @Override
    public long availableSystemMemory() {
        long overheadBytes = getBytesFromSystemProperty("es.total_memory_overhead_bytes", SERVER_CLI_OVERHEAD);
        return delegate.availableSystemMemory() - overheadBytes;
    }
}
