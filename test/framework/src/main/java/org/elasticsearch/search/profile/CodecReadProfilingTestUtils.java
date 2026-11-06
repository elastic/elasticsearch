/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.profile;

import static org.apache.lucene.tests.util.LuceneTestCase.assumeTrue;

/** Helpers for tests of codec read profiling, which {@link ProfileSelection#CODEC_READ_PROFILING_FEATURE_FLAG} gates. */
public final class CodecReadProfilingTestUtils {

    private CodecReadProfilingTestUtils() {}

    /**
     * Skips a test that needs codec read profiling when the feature flag is disabled, as it is by default in a release
     * build. A test of what happens without the flag checks {@link ProfileSelection#CODEC_READ_PROFILING_FEATURE_FLAG}
     * itself instead.
     */
    public static void assumeCodecReadProfilingEnabled() {
        assumeTrue(
            "codec read profiling needs the codec_read_profiling feature flag, disabled by default in release builds",
            ProfileSelection.CODEC_READ_PROFILING_FEATURE_FLAG.isEnabled()
        );
    }
}
