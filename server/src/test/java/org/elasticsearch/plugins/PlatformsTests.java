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
