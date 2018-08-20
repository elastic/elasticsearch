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

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.Shell.Result;

import java.util.regex.Pattern;

import static junit.framework.TestCase.assertTrue;
import static org.elasticsearch.packaging.util.Distribution.DEFAULT_DEB;
import static org.elasticsearch.packaging.util.Distribution.DEFAULT_RPM;
import static org.elasticsearch.packaging.util.Distribution.OSS_DEB;
import static org.elasticsearch.packaging.util.Distribution.OSS_RPM;
import static org.elasticsearch.packaging.util.FileUtils.getDistributionFile;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that linux packages correctly declare their dependencies and their conflicts
 */
public class PackageDependenciesTests extends PackagingTestCase {

    public void testDebDependencies() {
        assumeTrue(Platforms.isDPKG());

        final Shell sh = new Shell();

        final Result defaultResult = sh.run("dpkg -I " + getDistributionFile(DEFAULT_DEB));
        final Result ossResult = sh.run("dpkg -I " + getDistributionFile(OSS_DEB));

        assertTrue(Pattern.compile("(?m)^ Depends:.*bash.*").matcher(defaultResult.stdout).find());
        assertTrue(Pattern.compile("(?m)^ Depends:.*bash.*").matcher(ossResult.stdout).find());

        assertTrue(Pattern.compile("(?m)^ Conflicts: elasticsearch-oss$").matcher(defaultResult.stdout).find());
        assertTrue(Pattern.compile("(?m)^ Conflicts: elasticsearch$").matcher(ossResult.stdout).find());
    }

    public void testRpmDependencies() {
        assumeTrue(Platforms.isRPM());

        final Shell sh = new Shell();

        final Result defaultDeps = sh.run("rpm -qpR " + getDistributionFile(DEFAULT_RPM));
        final Result ossDeps = sh.run("rpm -qpR " + getDistributionFile(OSS_RPM));

        assertTrue(Pattern.compile("(?m)^/bin/bash\\s*$").matcher(defaultDeps.stdout).find());
        assertTrue(Pattern.compile("(?m)^/bin/bash\\s*$").matcher(ossDeps.stdout).find());

        final Result defaultConflicts = sh.run("rpm -qp --conflicts " + getDistributionFile(DEFAULT_RPM));
        final Result ossConflicts = sh.run("rpm -qp --conflicts " + getDistributionFile(OSS_RPM));

        assertTrue(Pattern.compile("(?m)^elasticsearch-oss\\s*$").matcher(defaultConflicts.stdout).find());
        assertTrue(Pattern.compile("(?m)^elasticsearch\\s*$").matcher(ossConflicts.stdout).find());
    }
}
