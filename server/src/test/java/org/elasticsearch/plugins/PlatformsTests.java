/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugins;

import org.apache.lucene.util.Constants;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;

public class PlatformsTests extends ESTestCase {

    public void testNativeControllerPath() {

        final Path nativeControllerPath = Platforms.nativeControllerPath(createTempDir());

        // The directory structure on macOS must match Apple's .app
        // structure or Gatekeeper may refuse to run the program
        if (Constants.MAC_OS_X) {
            String programName = nativeControllerPath.getFileName().toString();
            Path binDirectory = nativeControllerPath.getParent();
            assertEquals("MacOS", binDirectory.getFileName().toString());
            Path contentsDirectory = binDirectory.getParent();
            assertEquals("Contents", contentsDirectory.getFileName().toString());
            Path appDirectory = contentsDirectory.getParent();
            assertEquals(programName + ".app", appDirectory.getFileName().toString());
        } else {
            Path binDirectory = nativeControllerPath.getParent();
            assertEquals("bin", binDirectory.getFileName().toString());
        }
    }
}
