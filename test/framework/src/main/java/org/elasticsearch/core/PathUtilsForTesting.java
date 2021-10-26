/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.core.PathUtils;

import java.nio.file.FileSystem;

/**
 * Exposes some package private stuff in PathUtils for framework purposes only!
 */
public class PathUtilsForTesting {

    /** Sets a new default filesystem for testing */
    public static void setup() {
        installMock(LuceneTestCase.createTempDir().getFileSystem());
    }

    /** Installs a mock filesystem for testing */
    public static void installMock(FileSystem mock) {
        PathUtils.DEFAULT = mock;
    }

    /** Resets filesystem back to the real system default */
    public static void teardown() {
        PathUtils.DEFAULT = PathUtils.ACTUAL_DEFAULT;
    }
}
