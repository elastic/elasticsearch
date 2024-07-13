/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.preallocate;

import org.elasticsearch.common.filesystem.FileSystemNatives;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.OptionalLong;

import static org.hamcrest.Matchers.equalTo;

public class PreallocateTests extends ESTestCase {
    public void testPreallocate() throws Exception {
        Path cacheFile = createTempFile();
        long size = 1024 * 1024; // 1 MB
        Preallocate.preallocate(cacheFile, size);
        OptionalLong foundSize = FileSystemNatives.allocatedSizeInBytes(cacheFile);
        assertTrue(foundSize.isPresent());
        assertThat(foundSize.getAsLong(), equalTo(0));
    }
}
