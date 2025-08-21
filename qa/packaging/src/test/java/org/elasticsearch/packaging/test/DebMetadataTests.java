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
import org.elasticsearch.packaging.util.LintianResultParser;
import org.elasticsearch.packaging.util.LintianResultParser.Issue;
import org.elasticsearch.packaging.util.LintianResultParser.Result;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.packaging.util.FileUtils.getDistributionFile;
import static org.junit.Assume.assumeTrue;

public class DebMetadataTests extends PackagingTestCase {

    private final LintianResultParser lintianParser = new LintianResultParser();
    private static final List<String> IGNORED_TAGS = List.of(
        // Override syntax changes between lintian versions in a non-backwards compatible way, so we have to tolerate these.
        // Tag mismatched-override is a non-erasable tag which cannot be ignored with overrides, so we handle it here.
        "mismatched-override",
        // systemd-service-file-outside-lib has been incorrect and removed in the newer version on Lintian
        "systemd-service-file-outside-lib"
    );

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
            extraArgs = "--fail-on error,warning";
        }
        Shell.Result result = sh.runIgnoreExitCode(
            String.format(Locale.ROOT, "lintian %s %s", extraArgs, getDistributionFile(distribution()))
        );
        Result lintianResult = lintianParser.parse(result.stdout());
        // Unfortunately Lintian overrides syntax changes between Lintian versions in a non-backwards compatible
        // way, so we have to manage some exclusions outside the overrides file.
        if (lintianResult.isSuccess() == false) {
            List<Issue> importantIssues = lintianResult.issues()
                .stream()
                .filter(issue -> IGNORED_TAGS.contains(issue.tag()) == false)
                .toList();
            if (importantIssues.isEmpty() == false) {
                fail(
                    "Issues for DEB package found by Lintian:\n"
                        + importantIssues.stream().map(Record::toString).collect(Collectors.joining("\n"))
                );
            }
        }
    }

    public void test06Dependencies() {

        final Shell sh = new Shell();

        final Shell.Result result = sh.run("dpkg -I " + getDistributionFile(distribution()));

        TestCase.assertTrue(Pattern.compile("(?m)^ Depends:.*bash.*").matcher(result.stdout()).find());

        String oppositePackageName = "elasticsearch-oss";
        TestCase.assertTrue(Pattern.compile("(?m)^ Conflicts: " + oppositePackageName + "$").matcher(result.stdout()).find());
    }
}
