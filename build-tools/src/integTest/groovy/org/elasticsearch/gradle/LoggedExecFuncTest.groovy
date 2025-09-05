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


    def "failed tasks output logged to console"() {
        setup:
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          commandLine 'ls', 'wtf'
        }
        """
        when:
        def result = gradleRunner("loggedExec").buildAndFail()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.FAILED
        assertOutputContains(result.output, """\
            > Task :loggedExec FAILED
            Output for ls:""".stripIndent())
        assertOutputContains(result.output, "No such file or directory")
    }

    def "can capture output"() {
        setup:
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          commandLine 'echo', 'HELLO'
          getCaptureOutput().set(true)
          doLast {
            println 'OUTPUT ' + output
          }
        }

        """
        when:
        def result = gradleRunner("loggedExec").build()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
        result.getOutput().contains("OUTPUT HELLO")
    }



    def "can configure output indenting"() {
        setup:
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          getIndentingConsoleOutput().set("CUSTOM")
          commandLine('echo', '''
            HELLO
            Darkness
            my old friend''')
        }
        """
        when:
        def result = gradleRunner("loggedExec", '-q').build()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
        normalized(result.output) == '''
          [CUSTOM]             HELLO
          [CUSTOM]             Darkness
          [CUSTOM]             my old friend'''.stripIndent(9)
    }

    def "can provide standard input"() {
        setup:
        file('script.sh') << """
#!/bin/bash

# Read the user input

echo "Enter the user input: "
read userInput
echo "The user input is \$userInput"
"""
        buildFile << """
        import org.elasticsearch.gradle.LoggedExec
        tasks.register('loggedExec', LoggedExec) {
          getCaptureOutput().set(true)
          commandLine 'bash', 'script.sh'
          getStandardInput().set('FooBar')
          doLast {
            println output
          }
        }
        """
        when:
        def result = gradleRunner("loggedExec").build()
        then:
        result.task(':loggedExec').outcome == TaskOutcome.SUCCESS
        result.getOutput().contains("The user input is FooBar")
    }
}
