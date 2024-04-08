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
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import java.util.Locale;
import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.FileUtils.getDistributionFile;
import static org.junit.Assume.assumeTrue;

public class DebMetadataTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only deb", distribution.packaging == Distribution.Packaging.DEB);
    }

    public void test05CheckLintian() {
        String extraArgs = "";
        final String helpText = sh.run("lintian --help").stdout();
        if (helpText.contains("--fail-on-warnings")) {
            extraArgs = "--fail-on-warnings";
        } else if (helpText.contains("--fail-on error")) {
            extraArgs = "--fail-on warning";
            // Recent lintian versions are picky about malformed or mismatched overrides.
            // Unfortunately override syntax changes between lintian versions in a non-backwards compatible
            // way, so we have to tolerate these (or maintain separate override files per lintian version).
            if (helpText.contains("--suppress-tags")) {
                extraArgs += " --suppress-tags malformed-override,mismatched-override";
            }
        }
        sh.run(String.format(Locale.ROOT, "lintian %s %s", extraArgs, FileUtils.getDistributionFile(distribution())));
    }

    public void test06Dependencies() {

        final Shell sh = new Shell();

        final Shell.Result result = sh.run("dpkg -I " + getDistributionFile(distribution()));

        TestCase.assertTrue(Pattern.compile("(?m)^ Depends:.*bash.*").matcher(result.stdout()).find());

        String oppositePackageName = "elasticsearch-oss";
        TestCase.assertTrue(Pattern.compile("(?m)^ Conflicts: " + oppositePackageName + "$").matcher(result.stdout()).find());
    }
}
