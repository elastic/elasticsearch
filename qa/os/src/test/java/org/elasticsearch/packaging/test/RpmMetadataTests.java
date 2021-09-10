/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

public class RpmMetadataTests extends PackagingTestCase {

    @Before
    public void filterDistros() {
        assumeTrue("only rpm", distribution.packaging == Distribution.Packaging.RPM);
    }

    public void test11Dependencies() {
        // TODO: rewrite this test to not use a real second distro to try and install
        assumeTrue(Platforms.isRPM());

        final Shell sh = new Shell();

        final Shell.Result deps = sh.run("rpm -qpR " + getDistributionFile(distribution()));

        TestCase.assertTrue(Pattern.compile("(?m)^/bin/bash\\s*$").matcher(deps.stdout).find());

        final Shell.Result conflicts = sh.run("rpm -qp --conflicts " + getDistributionFile(distribution()));

        String oppositePackageName = "elasticsearch-oss";
        TestCase.assertTrue(Pattern.compile("(?m)^" + oppositePackageName + "\\s*$").matcher(conflicts.stdout).find());
    }
}
