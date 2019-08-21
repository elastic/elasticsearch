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

import junit.framework.TestCase;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.FileUtils.getDistributionFile;
import static org.junit.Assume.assumeTrue;

public class PackageConflictTests extends PackagingTestCase {

    private Shell sh;

    @Before
    public void onlyCompatibleDistributions() throws Exception {
        assumeTrue("only compatible distributions", distribution().packaging.compatible);
        assumeTrue("rpm or deb",
            distribution().packaging == Distribution.Packaging.DEB || distribution().packaging == Distribution.Packaging.RPM);
        sh = newShell();
    }

    public void test11DebDependencies() {
        // TODO: rewrite this test to not use a real second distro to try and install
        assumeTrue(Platforms.isDPKG());

        final Shell sh = new Shell();

        final Shell.Result result = sh.run("dpkg -I " + getDistributionFile(distribution()));

        TestCase.assertTrue(Pattern.compile("(?m)^ Depends:.*bash.*").matcher(result.stdout).find());

        String oppositePackageName = "elasticsearch";
        if (distribution().isDefault()) {
            oppositePackageName += "-oss";
        }

        TestCase.assertTrue(Pattern.compile("(?m)^ Conflicts: " + oppositePackageName + "$").matcher(result.stdout).find());
    }

    public void test11RpmDependencies() {
        // TODO: rewrite this test to not use a real second distro to try and install
        assumeTrue(Platforms.isRPM());

        final Shell sh = new Shell();

        final Shell.Result deps = sh.run("rpm -qpR " + getDistributionFile(distribution()));

        TestCase.assertTrue(Pattern.compile("(?m)^/bin/bash\\s*$").matcher(deps.stdout).find());

        final Shell.Result conflicts = sh.run("rpm -qp --conflicts " + getDistributionFile(distribution()));

        String oppositePackageName = "elasticsearch";
        if (distribution().isDefault()) {
            oppositePackageName += "-oss";
        }

        TestCase.assertTrue(Pattern.compile("(?m)^" + oppositePackageName + "\\s*$").matcher(conflicts.stdout).find());
    }
}
