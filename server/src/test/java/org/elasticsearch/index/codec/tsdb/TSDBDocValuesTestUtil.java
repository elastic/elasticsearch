/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.test.ESTestCase;

/**
 * Static helpers for TSDB doc values tests. Provides randomized
 * {@link BinaryDVCompressionMode} and numeric block shift values, and initializes
 * Elasticsearch logging on class load.
 */
public final class TSDBDocValuesTestUtil {

    private static final int NUMERIC_BLOCK_SHIFT = 7;
    private static final int NUMERIC_LARGE_BLOCK_SHIFT = 9;

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    private TSDBDocValuesTestUtil() {}

    /** Returns a uniformly random {@link BinaryDVCompressionMode}. */
    public static BinaryDVCompressionMode randomBinaryCompressionMode() {
        BinaryDVCompressionMode[] modes = BinaryDVCompressionMode.values();
        return modes[ESTestCase.random().nextInt(modes.length)];
    }

    /** Returns a random numeric block shift, either the default ({@code 7}) or the large layout ({@code 9}). */
    public static int randomNumericBlockSize() {
        return ESTestCase.random().nextBoolean() ? NUMERIC_LARGE_BLOCK_SHIFT : NUMERIC_BLOCK_SHIFT;
    }
}
