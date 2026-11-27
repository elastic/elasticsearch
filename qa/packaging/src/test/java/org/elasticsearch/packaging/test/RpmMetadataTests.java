/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import junit.framework.TestCase;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import java.util.List;
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

        TestCase.assertTrue(Pattern.compile("(?m)^/bin/bash\\s*$").matcher(deps.stdout()).find());

        final Shell.Result conflicts = sh.run("rpm -qp --conflicts " + getDistributionFile(distribution()));

        String oppositePackageName = "elasticsearch-oss";
        TestCase.assertTrue(Pattern.compile("(?m)^" + oppositePackageName + "\\s*$").matcher(conflicts.stdout()).find());
    }

    /**
     * Config files must be flagged both {@code config} and {@code noreplace} so that an upgrade keeps administrator edits in place
     * and writes the packaged version to {@code .rpmnew} instead of overwriting them.
     */
    public void test12ConfigFilesAreNoReplace() {
        assumeTrue(Platforms.isRPM());

        final Shell sh = new Shell();
        final Shell.Result result = sh.run("rpm -qp --qf '[%{FILEFLAGS:fflags} %{FILENAMES}\\n]' " + getDistributionFile(distribution()));

        for (String configFile : List.of(
            "/etc/elasticsearch/elasticsearch.yml",
            "/etc/elasticsearch/jvm.options",
            "/etc/elasticsearch/log4j2.properties",
            "/etc/sysconfig/elasticsearch",
            "/usr/lib/systemd/system/elasticsearch.service",
            "/usr/lib/sysctl.d/elasticsearch.conf"
        )) {
            TestCase.assertTrue(
                "expected [" + configFile + "] to be flagged config and noreplace in:\n" + result.stdout(),
                Pattern.compile("(?m)^cn " + Pattern.quote(configFile) + "$").matcher(result.stdout()).find()
            );
        }
    }

    /**
     * The package must own the intermediate directories of its installation tree so that the package manager removes them on
     * uninstallation instead of leaving them behind.
     */
    public void test13InstallationDirectoriesAreOwned() {
        assumeTrue(Platforms.isRPM());

        final Shell sh = new Shell();
        final Shell.Result result = sh.run("rpm -qp --qf '[%{FILEMODES:perms} %{FILENAMES}\\n]' " + getDistributionFile(distribution()));

        for (String directory : List.of(
            "/usr/share/elasticsearch/bin",
            "/usr/share/elasticsearch/lib",
            "/usr/share/elasticsearch/modules",
            "/usr/share/elasticsearch/jdk"
        )) {
            TestCase.assertTrue(
                "expected directory entry for [" + directory + "] in:\n" + result.stdout(),
                Pattern.compile("(?m)^d\\S* " + Pattern.quote(directory) + "$").matcher(result.stdout()).find()
            );
        }
    }
}
