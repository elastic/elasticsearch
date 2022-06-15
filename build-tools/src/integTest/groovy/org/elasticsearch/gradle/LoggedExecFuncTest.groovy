/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle

import org.elasticsearch.gradle.fixtures.AbstractGradleFuncTest
import org.gradle.testkit.runner.TaskOutcome
import spock.lang.Ignore
import spock.lang.IgnoreIf
import spock.lang.Unroll
import spock.util.environment.OperatingSystem

@IgnoreIf({ os.isWindows() })
class LoggedExecFuncTest extends AbstractGradleFuncTest {

    def setup() {
        buildFile << """
        // we need apply any custom plugin 
        // to add build-logic to the build classpath
        plugins {
            id 'elasticsearch.distribution-download'
        }
        """
    }

    @Unroll
    def "can configure spooling #spooling"() {
        setup:
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          commandLine 'ls', '-lh'
          spoolOutput = $spooling
        }
        """
        when:
        def result = gradleRunner("loggedExec").build()
        then:
//        assertOutputContains(result.getOutput(), "Configuration cache entry stored.")
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
        file("build/buffered-output/loggedExec").exists() == false

////
//        when:
//        result = gradleRunner("loggedExec").buildAndFail()
//        then:
//        result.task(':loggedExec').outcome == TaskOutcome.FAILED
////        assertOutputContains(result.getOutput(), "Configuration cache entry reused.")

        where:
        spooling << [false, true]
    }

    @Unroll
    @Ignore
    def "can run #setup setup with configuration cache"() {
        setup:
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          commandLine 'ls', '-lh'
          $config
        }
        """
        when:
        def result = gradleRunner("loggedExec", '--configuration-cache').build()
        then:
        assertOutputContains(result.getOutput(), "Configuration cache entry stored.")

        when:
        result = gradleRunner("loggedExec", '--configuration-cache').build()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
        assertOutputContains(result.getOutput(), "Configuration cache entry reused.")
        where:
        setup                   | config
        "basic"                 | ""
    }
}
