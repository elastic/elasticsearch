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
          executable = 'ls'
          getArgs().add('-lh')
          spoolOutput = $spooling
        }
        """
        when:
        def result = gradleRunner("loggedExec").build()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
        file("build/buffered-output/loggedExec").exists() == spooling
        where:
        spooling << [false, true]
    }

    def "captures output"() {
        setup:
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          executable = 'ls'
          getArgs().add('-lh')
          doLast {
            println 'OUTPUT ' + output
          }
        }
       
        """
        when:
        def result = gradleRunner("loggedExec").build()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
    }
}
