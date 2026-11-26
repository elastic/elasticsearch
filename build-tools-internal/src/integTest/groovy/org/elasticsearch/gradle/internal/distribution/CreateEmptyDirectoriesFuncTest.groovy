/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome

/**
 * Runs a {@code Copy} task using {@link CreateEmptyDirectories} the way {@code distribution/packages/build.gradle}
 * does. The second run reuses the configuration cache entry and forces the task to execute, which is the scenario in
 * which the previous script based {@code mkdir()} action failed.
 */
class CreateEmptyDirectoriesFuncTest extends AbstractGradleFuncTest {

    def setup() {
        file("src/file.txt") << "content"
        buildFile << """
            import org.elasticsearch.gradle.internal.distribution.CreateEmptyDirectories

            plugins {
              id 'base'
              // we need to add one of our plugins to get things on this classpath
              id 'elasticsearch.global-build-info' apply false
            }

            tasks.register("processFiles", Copy) {
              from 'src'
              into 'build/packaging'
              doLast(new CreateEmptyDirectories(['var/log/elasticsearch', 'elasticsearch/jvm.options.d']))
            }
        """
    }

    def "creates empty directories when running from a reused configuration cache entry"() {
        when:
        def result1 = gradleRunner("processFiles").build()

        then:
        result1.task(":processFiles").outcome == TaskOutcome.SUCCESS
        assertEmptyDirectoriesCreated()

        when:
        file("build/packaging").deleteDir()
        def result2 = gradleRunner("processFiles", "--rerun-tasks").build()

        then:
        result2.output.contains("Configuration cache entry reused.")
        result2.task(":processFiles").outcome == TaskOutcome.SUCCESS
        assertEmptyDirectoriesCreated()
    }

    def "fails for tasks that are not copy tasks"() {
        given:
        buildFile << """
            tasks.register("notACopy") {
              doLast(new CreateEmptyDirectories(['var/log/elasticsearch']))
            }
        """

        when:
        def result = gradleRunner("notACopy").buildAndFail()

        then:
        result.output.contains("Task [:notACopy] must be a Copy task to create empty directories in")
    }

    private void assertEmptyDirectoriesCreated() {
        assert file("build/packaging/file.txt").text == "content"
        assert file("build/packaging/var/log/elasticsearch").directory
        assert file("build/packaging/var/log/elasticsearch").list().length == 0
        assert file("build/packaging/elasticsearch/jvm.options.d").directory
        assert file("build/packaging/elasticsearch/jvm.options.d").list().length == 0
    }
}
