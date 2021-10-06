/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.jarhell;

import org.elasticsearch.gradle.internal.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;
import org.junit.Before;

import static org.elasticsearch.gradle.internal.test.TestClasspathUtils.setupJarJdkClasspath;

public class ThirdPartyAuditTaskIT extends GradleIntegrationTestCase {

    @Override
    public String projectName() {
        return "thirdPartyAudit";
    }

    @Before
    public void setUp() throws Exception {
        // Build the sample jars
        getGradleRunner().withArguments(":sample_jars:build", "-s").build();
        // propagate jdkjarhell jar
    }

    public void testElasticsearchIgnored() {
        BuildResult result = getGradleRunner().withArguments(
                ":clean",
                ":empty",
                "-s",
                "-PcompileOnlyGroup=elasticsearch.gradle:broken-log4j",
                "-PcompileOnlyVersion=0.0.1",
                "-PcompileGroup=elasticsearch.gradle:dummy-io",
                "-PcompileVersion=0.0.1"
        ).build();
        assertTaskNoSource(result, ":empty");
        assertNoDeprecationWarning(result);
    }

    public void testViolationFoundAndCompileOnlyIgnored() {
        setupJarJdkClasspath(getProjectDir());

        BuildResult result = getGradleRunner().withArguments(
                ":clean",
                ":absurd",
                "-s",
                "-PcompileOnlyGroup=other.gradle:broken-log4j",
                "-PcompileOnlyVersion=0.0.1",
                "-PcompileGroup=other.gradle:dummy-io",
                "-PcompileVersion=0.0.1"
        ).buildAndFail();

        assertTaskFailed(result, ":absurd");
        assertOutputContains(result.getOutput(), "Classes with violations:", "  * TestingIO", "> Audit of third party dependencies failed");
        assertOutputMissing(result.getOutput(), "Missing classes:");
        assertNoDeprecationWarning(result);
    }

    public void testClassNotFoundAndCompileOnlyIgnored() {
        setupJarJdkClasspath(getProjectDir());
        BuildResult result = getGradleRunner().withArguments(
                ":clean",
                ":absurd",
                "-s",
                "-PcompileGroup=other.gradle:broken-log4j",
                "-PcompileVersion=0.0.1",
                "-PcompileOnlyGroup=other.gradle:dummy-io",
                "-PcompileOnlyVersion=0.0.1"
        ).buildAndFail();
        assertTaskFailed(result, ":absurd");

        assertOutputContains(
                result.getOutput(),
                "Missing classes:",
                "  * org.apache.logging.log4j.LogManager",
                "> Audit of third party dependencies failed"
        );
        assertOutputMissing(result.getOutput(), "Classes with violations:");
        assertNoDeprecationWarning(result);
    }

    public void testJarHellWithJDK() {
        setupJarJdkClasspath(getProjectDir(), "> Audit of third party dependencies failed:" +
                "   Jar Hell with the JDK:" +
                "    * java.lang.String"
        );
        BuildResult result = getGradleRunner().withArguments(
                ":clean",
                ":absurd",
                "-s",
                "-PcompileGroup=other.gradle:jarhellJdk",
                "-PcompileVersion=0.0.1",
                "-PcompileOnlyGroup=other.gradle:dummy-io",
                "-PcompileOnlyVersion=0.0.1"
        ).buildAndFail();
        assertTaskFailed(result, ":absurd");

        assertOutputContains(
                result.getOutput(),
                "> Audit of third party dependencies failed:",
                "   Jar Hell with the JDK:",
                "    * java.lang.String"
        );
        assertOutputMissing(result.getOutput(), "Classes with violations:");
        assertNoDeprecationWarning(result);
    }

    public void testElasticsearchIgnoredWithViolations() {
        BuildResult result = getGradleRunner().withArguments(
                ":clean",
                ":absurd",
                "-s",
                "-PcompileOnlyGroup=elasticsearch.gradle:broken-log4j",
                "-PcompileOnlyVersion=0.0.1",
                "-PcompileGroup=elasticsearch.gradle:dummy-io",
                "-PcompileVersion=0.0.1"
        ).build();
        assertTaskNoSource(result, ":absurd");
        assertNoDeprecationWarning(result);
    }

}
