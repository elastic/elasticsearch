/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import spock.lang.IgnoreIf

import static org.elasticsearch.gradle.fixtures.JdkToolchainTestFixture.withMockedJdkDownload

/**
 * Integration tests for {@link org.elasticsearch.gradle.fixtures.JdkToolchainTestFixture}.
 * Verifies that the fixture runs a Gradle build with a pre-installed fake JDK path and that
 * the build succeeds. Full toolchain resolution and ES_JAVA_HOME override are covered by
 * {@link TestClustersPluginFuncTest#override jdk usage via ES_JAVA_HOME for known jdk os incompatibilities}.
 */
@IgnoreIf({ os.isWindows() })
class JdkToolchainTestFixtureFuncTest extends AbstractGradleFuncTest {

    def "build with mocked JDK path succeeds"() {
        when:
        def runner = gradleRunner('tasks', '-i', '-Dorg.gradle.java.installations.auto-detect=false')
        def result = withMockedJdkDownload(runner, 17, 'eclipse_adoptium')

        then:
        result.output.contains('BUILD SUCCESSFUL')
    }
}
