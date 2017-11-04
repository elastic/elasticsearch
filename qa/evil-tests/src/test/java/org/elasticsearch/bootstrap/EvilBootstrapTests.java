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
package org.elasticsearch.bootstrap;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;

/**
 * This has to be an evil test because it checks file permissions, and security manager does not allow that.
 */
public class EvilBootstrapTests extends ESTestCase {

    private static boolean isPosix;

    @BeforeClass
    public static void checkPosix() throws IOException {
        isPosix = Files.getFileAttributeView(createTempFile(), PosixFileAttributeView.class) != null;
    }

    public void testTmpDirPerms() throws IOException {
        // %TEMP% directories are restricted by default on Windows, so in this case it doesn't
        // really matter what permissions the directory we create beneath java.io.tmpdir have.
        // And if the user has changed java.io.tmpdir then they should take responsibility for
        // ensuring the chosen location is as secure as they require.
        assumeTrue("Temp directory permissions not set on Windows", isPosix);

        final Path tmpPath = Bootstrap.makeSecureTmpPath(createTempDir());

        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(tmpPath);
        assertThat(perms,
            containsInAnyOrder(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        assertThat(perms, not(contains(PosixFilePermission.GROUP_READ)));
        assertThat(perms, not(contains(PosixFilePermission.GROUP_WRITE)));
        assertThat(perms, not(contains(PosixFilePermission.GROUP_EXECUTE)));
        assertThat(perms, not(contains(PosixFilePermission.OTHERS_READ)));
        assertThat(perms, not(contains(PosixFilePermission.OTHERS_WRITE)));
        assertThat(perms, not(contains(PosixFilePermission.OTHERS_EXECUTE)));
    }
}
