/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io;

import org.apache.lucene.util.LuceneTestCase;

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
